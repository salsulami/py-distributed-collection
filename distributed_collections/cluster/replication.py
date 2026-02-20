from __future__ import annotations

import queue
import threading
import time
from typing import Any

from ..config import NodeAddress
from ..exceptions import ClusterNotRunningError
from ..protocol import MessageKind
from ..transport import request_response

_REPLICATION_SENTINEL: tuple[NodeAddress, dict[str, Any], int] | None = None


class ClusterReplicationMixin:
    """
    Asynchronous replication queue, batching, and retry behavior.
    """

    def _start_replication_workers(self) -> None:
        """Start asynchronous replication worker threads."""
        self._replication_stop.clear()
        self._replication_threads.clear()
        for index in range(self.config.replication.worker_threads):
            thread = threading.Thread(
                target=self._replication_worker_loop,
                name=f"cluster-replication-{index}",
                daemon=True,
            )
            thread.start()
            self._replication_threads.append(thread)

    def _stop_replication_workers(self) -> None:
        """Stop replication worker threads."""
        self._replication_stop.set()
        for _ in self._replication_threads:
            self._replication_queue.put(_REPLICATION_SENTINEL)
        for thread in self._replication_threads:
            thread.join(timeout=1.0)
        self._replication_threads.clear()

    def _enqueue_replication(self, payload: dict[str, Any]) -> None:
        """
        Enqueue replication tasks with backpressure handling.
        """
        for peer in self.members():
            task = (peer, payload, 1)
            try:
                self._replication_queue.put(
                    task,
                    timeout=self.config.replication.enqueue_timeout_seconds,
                )
                self._inc_stat("replication_enqueued")
            except queue.Full:
                if self.config.replication.drop_on_overflow:
                    self._inc_stat("replication_backpressure_drops")
                    self._inc_stat("dropped_messages")
                    continue
                raise ClusterNotRunningError(
                    "Replication queue overflow; refusing operation to apply flow control."
                ) from None

    def _replication_worker_loop(self) -> None:
        """
        Replication worker: drains queue, batches by peer, and retries failures.
        """
        while not self._replication_stop.is_set():
            try:
                first = self._replication_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if first is _REPLICATION_SENTINEL:
                return

            batch: list[tuple[NodeAddress, dict[str, Any], int]] = [first]
            while len(batch) < self.config.replication.batch_size:
                try:
                    nxt = self._replication_queue.get_nowait()
                except queue.Empty:
                    break
                if nxt is _REPLICATION_SENTINEL:
                    self._replication_queue.put(_REPLICATION_SENTINEL)
                    break
                batch.append(nxt)

            grouped: dict[NodeAddress, list[tuple[dict[str, Any], int]]] = {}
            for peer, payload, attempt in batch:
                grouped.setdefault(peer, []).append((payload, attempt))

            for peer, items in grouped.items():
                payloads = [payload for payload, _ in items]
                ok = self._deliver_peer_batch(peer, payloads)
                if ok:
                    self._clear_member_failure(peer)
                    self._inc_stat("replication_success", delta=len(items))
                    continue

                self._inc_stat("replication_failures", delta=len(items))
                self._record_member_failure(peer)
                for payload, attempt in items:
                    if attempt >= self.config.replication.max_retries:
                        continue
                    if self._replication_stop.is_set():
                        continue
                    delay = min(
                        self.config.replication.max_backoff_seconds,
                        self.config.replication.initial_backoff_seconds * (2 ** (attempt - 1)),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    try:
                        self._replication_queue.put(
                            (peer, payload, attempt + 1),
                            timeout=self.config.replication.enqueue_timeout_seconds,
                        )
                    except queue.Full:
                        self._inc_stat("replication_backpressure_drops")

    def _deliver_peer_batch(self, peer: NodeAddress, payloads: list[dict[str, Any]]) -> bool:
        """
        Deliver one or many operation payloads to a peer in one request.
        """
        if not payloads:
            return True
        if len(payloads) == 1:
            return self._deliver_single_operation(
                peer,
                payloads[0],
                timeout_seconds=self.config.socket_timeout_seconds,
            )
        try:
            response = request_response(
                peer,
                self._make_message(MessageKind.OPERATION_BATCH, {"operations": payloads}),
                self.config.socket_timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
            if response.get("kind") == MessageKind.ERROR.value:
                return False
            return True
        except Exception:
            return False

    def _deliver_single_operation(
        self,
        peer: NodeAddress,
        payload: dict[str, Any],
        *,
        timeout_seconds: float,
    ) -> bool:
        """
        Deliver one operation to one peer and return success status.
        """
        try:
            response = request_response(
                peer,
                self._make_message(MessageKind.OPERATION, payload),
                timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
            if response.get("kind") == MessageKind.ERROR.value:
                return False
            return True
        except Exception:
            return False
