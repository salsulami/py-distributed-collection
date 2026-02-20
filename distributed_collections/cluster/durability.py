from __future__ import annotations

import threading
import time
from typing import Any

from ..config import NodeAddress
from ..protocol import MessageKind
from ..transport import request_response


class ClusterDurabilityMixin:
    """
    Snapshot, WAL, and state-sync durability lifecycle helpers.
    """

    def _load_snapshot_if_enabled(self) -> None:
        """
        Load persisted snapshot state and metadata if configured.
        """
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        try:
            payload = self._persistence.load()
            if payload is None:
                return
            state: dict[str, Any]
            metadata: dict[str, Any]
            has_cp_payload = False
            if "state" in payload and isinstance(payload.get("state"), dict):
                state = dict(payload["state"])
                raw_meta = payload.get("metadata", {})
                metadata = dict(raw_meta) if isinstance(raw_meta, dict) else {}
                has_cp_payload = "cp" in payload
                raw_cp = payload.get("cp", {})
                cp_payload = dict(raw_cp) if isinstance(raw_cp, dict) else {}
            else:
                # Backward compatibility with old snapshot format.
                state = payload
                metadata = {}
                cp_payload = {}
            self._store.load_snapshot(state)
            if has_cp_payload:
                self._load_cp_snapshot_payload(cp_payload)
            with self._consensus_lock:
                self._last_applied_index = int(metadata.get("last_applied_index", 0))
                self._next_log_index = max(self._last_applied_index + 1, 1)
                self._term = int(metadata.get("term", self._term))
            self._inc_stat("snapshot_load_success")
        except Exception:
            self._inc_stat("snapshot_load_failures")

    def _replay_wal_if_enabled(self) -> None:
        """
        Replay WAL entries after snapshot recovery.
        """
        if self._store_is_centralized:
            return
        if self._wal is None:
            return
        try:
            entries = self._wal.replay()
            sorted_entries = sorted(
                (
                    item.get("operation")
                    for item in entries
                    if isinstance(item, dict) and isinstance(item.get("operation"), dict)
                ),
                key=lambda op: int(op.get("log_index", 0)),
            )
            for payload in sorted_entries:
                log_index = int(payload.get("log_index", 0))
                if log_index <= self._last_applied_index:
                    continue
                if str(payload.get("collection", "")) == "topic":
                    continue
                self._apply_payload(payload, source="wal_replay")
                with self._consensus_lock:
                    self._last_applied_index = max(self._last_applied_index, log_index)
                    self._next_log_index = max(self._next_log_index, self._last_applied_index + 1)
            self._inc_stat("wal_replay_success")
        except Exception:
            self._inc_stat("wal_replay_failures")

    def _append_wal_entry(self, payload: dict[str, Any]) -> None:
        """
        Append operation payload to WAL when enabled.
        """
        if self._is_centralized_data_operation(str(payload.get("collection", ""))):
            return
        if self._wal is None:
            return
        if str(payload.get("collection", "")) == "topic":
            return
        try:
            self._wal.append({"operation": payload})
            self._ops_since_checkpoint += 1
            self._inc_stat("wal_append_success")
        except Exception:
            self._inc_stat("wal_append_failures")

    def _schedule_persist(self) -> None:
        """Schedule asynchronous snapshot persistence flush."""
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        self._persist_request.set()

    def _start_persistence_worker(self) -> None:
        """Start background snapshot flush worker."""
        if self._persistence is None:
            return
        self._persist_stop.clear()
        self._persist_request.clear()
        self._persist_thread = threading.Thread(
            target=self._persistence_worker_loop,
            name="cluster-persistence-worker",
            daemon=True,
        )
        self._persist_thread.start()

    def _stop_persistence_worker(self, *, flush: bool) -> None:
        """Stop persistence worker and optionally flush one final snapshot."""
        if self._persistence is None:
            return
        self._persist_stop.set()
        self._persist_request.set()
        if self._persist_thread:
            self._persist_thread.join(timeout=1.0)
            self._persist_thread = None
        if flush:
            self._save_snapshot_now()

    def _persistence_worker_loop(self) -> None:
        """Persistence worker event loop."""
        while not self._persist_stop.is_set():
            signaled = self._persist_request.wait(timeout=0.5)
            if not signaled:
                continue
            self._persist_request.clear()
            self._save_snapshot_now()

    def _save_snapshot_now(self) -> None:
        """
        Persist snapshot + metadata and checkpoint WAL when due.
        """
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        try:
            self._persistence.save(self._snapshot_payload())
            self._inc_stat("snapshot_save_success")
            if (
                self._wal is not None
                and self._ops_since_checkpoint >= self.config.wal.checkpoint_interval_operations
            ):
                self._wal.truncate()
                self._ops_since_checkpoint = 0
        except Exception:
            self._inc_stat("snapshot_save_failures")

    def _snapshot_payload(self) -> dict[str, Any]:
        """
        Build persistence payload containing state and metadata.
        """
        with self._consensus_lock:
            metadata = {
                "term": self._term,
                "last_applied_index": self._last_applied_index,
                "node_id": self.node_id,
                "timestamp_ms": int(time.time() * 1000),
            }
        state = {} if self._store_is_centralized else self._store.create_snapshot()
        return {"state": state, "metadata": metadata, "cp": self._cp_snapshot_payload()}

    def _handshake_peer(self, peer: NodeAddress) -> dict[str, Any] | None:
        """
        Perform handshake with peer and return response payload.
        """
        request = self._make_message(
            MessageKind.HANDSHAKE,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "protocol_min": self.config.upgrade.min_compatible_protocol_version,
                "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            },
        )
        try:
            response = request_response(
                peer,
                request,
                self.config.socket_timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
        except Exception:
            return None
        if response.get("kind") != MessageKind.HANDSHAKE_ACK.value:
            return None
        payload = response.get("payload")
        if not isinstance(payload, dict):
            return None
        peer_min = int(payload.get("protocol_min", 1))
        peer_max = int(payload.get("protocol_max", 1))
        if not self._protocol_ranges_overlap(peer_min, peer_max):
            return None
        return payload

    def _sync_from_peer_list(self, peers: list[NodeAddress]) -> None:
        """
        Request snapshot state from first responsive peer and load locally.
        """
        request = self._make_message(MessageKind.STATE_REQUEST, {})
        for peer in peers:
            try:
                response = request_response(
                    peer,
                    request,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                    ssl_context=self._client_ssl_context,
                    server_hostname=self.config.tls.server_hostname,
                    min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                    max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
                )
            except Exception:
                continue
            if response.get("kind") != MessageKind.STATE_RESPONSE.value:
                continue
            payload = response.get("payload")
            if not isinstance(payload, dict):
                continue
            snapshot_payload = payload.get("snapshot")
            if not isinstance(snapshot_payload, dict):
                continue
            self._load_snapshot_payload(snapshot_payload)
            self._schedule_persist()
            self._trace("state_synced_from_peer", peer=str(peer))
            return

    def _load_snapshot_payload(self, payload: dict[str, Any]) -> None:
        """
        Load snapshot payload returned by peer state sync.
        """
        has_cp_payload = False
        if "state" in payload and isinstance(payload.get("state"), dict):
            state = dict(payload["state"])
            metadata_raw = payload.get("metadata", {})
            metadata = dict(metadata_raw) if isinstance(metadata_raw, dict) else {}
            has_cp_payload = "cp" in payload
            raw_cp = payload.get("cp", {})
            cp_payload = dict(raw_cp) if isinstance(raw_cp, dict) else {}
        else:
            state = payload
            metadata = {}
            cp_payload = {}
        if not self._store_is_centralized:
            self._store.load_snapshot(state)
        if has_cp_payload:
            self._load_cp_snapshot_payload(cp_payload)
        with self._consensus_lock:
            self._last_applied_index = int(metadata.get("last_applied_index", self._last_applied_index))
            self._next_log_index = max(self._last_applied_index + 1, self._next_log_index)
            self._term = max(self._term, int(metadata.get("term", self._term)))

    def _protocol_ranges_overlap(self, peer_min: int, peer_max: int) -> bool:
        """
        Return true when local and peer protocol compatibility ranges overlap.
        """
        local_min = self.config.upgrade.min_compatible_protocol_version
        local_max = self.config.upgrade.max_compatible_protocol_version
        return max(local_min, peer_min) <= min(local_max, peer_max)
