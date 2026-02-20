from __future__ import annotations

import time
import uuid
from typing import Any

from ..config import ConsistencyMode
from ..exceptions import ClusterNotRunningError
from ..protocol import MessageKind
from ..transport import request_response


class ClusterOperationMixin:
    """
    Local/remote operation application and commit consistency logic.
    """

    def _submit_local_operation(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> Any:
        """
        Submit one local mutation under configured consistency policy.
        """
        self._ensure_running()

        operation: dict[str, Any] = {
            "collection": collection,
            "name": name,
            "action": action,
        }
        if values:
            operation.update(values)

        if self._is_cp_operation(collection) and not self.config.cp.enabled:
            raise ClusterNotRunningError("CP subsystem is disabled.")

        mode = self._effective_consistency_mode(collection)
        if self.config.consensus.enabled and mode != ConsistencyMode.BEST_EFFORT:
            if not self._is_leader():
                return self._forward_operation_to_leader(operation)
            if self.config.consensus.require_majority_for_writes and not self._leader_lease_valid():
                self._inc_stat("quorum_write_failures")
                raise ClusterNotRunningError(
                    "Leader quorum lease expired; refusing write to avoid split-brain divergence."
                )

        return self._execute_leader_operation(operation, origin_node_id=self.node_id)

    def _execute_leader_operation(
        self,
        operation: dict[str, Any],
        *,
        origin_node_id: str,
    ) -> Any:
        """
        Execute one operation on leader and replicate using configured mode.
        """
        payload = self._build_operation_payload(operation, origin_node_id=origin_node_id)
        op_id = str(payload["op_id"])
        if not self._record_operation(op_id):
            return None

        collection_name = str(payload.get("collection", ""))
        self._append_wal_entry(payload)
        result = self._apply_payload(payload, source="local")
        self._inc_stat("local_mutations")
        self._schedule_persist()

        mode = self._effective_consistency_mode(collection_name)
        if self._is_centralized_data_operation(collection_name):
            return result

        # Commit strategy by consistency mode.
        required = self._required_ack_count(mode)
        if mode == ConsistencyMode.BEST_EFFORT:
            self._enqueue_replication(payload)
            return result

        acks = 1 + self._replicate_operation_sync(
            payload,
            required_acks=required,
            timeout_seconds=self.config.consistency.write_timeout_seconds,
        )
        self._enqueue_replication(payload)
        if acks < required:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError(
                f"Write commit failed; acknowledgements {acks} below required {required}."
            )
        return result

    def _forward_operation_to_leader(self, operation: dict[str, Any]) -> Any:
        """
        Forward write request to active leader for linearizable commit.
        """
        leader = self._resolve_leader_address()
        if leader is None and self.config.consensus.enabled:
            self._start_election()
            leader = self._resolve_leader_address()
            if leader == self.config.advertise_address:
                return self._execute_leader_operation(operation, origin_node_id=self.node_id)
        if leader is None:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("No known leader to forward operation.")

        request = self._make_message(
            MessageKind.FORWARD_OPERATION,
            {"operation": operation},
        )
        response = request_response(
            leader,
            request,
            self.config.consistency.write_timeout_seconds,
            security_token=self.config.security.shared_token,
            ssl_context=self._client_ssl_context,
            server_hostname=self.config.tls.server_hostname,
            min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
            max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
        )
        if response.get("kind") != MessageKind.FORWARD_OPERATION_RESULT.value:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("Leader returned unexpected forwarded-write response.")
        payload = response.get("payload", {})
        if not isinstance(payload, dict):
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("Leader returned malformed forwarded-write payload.")
        if payload.get("status") != "ok":
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError(str(payload.get("reason", "forwarded write rejected")))
        self._inc_stat("forwarded_writes")
        return payload.get("result")

    def _handle_remote_operation(self, payload: dict[str, Any], *, sender_node_id: str) -> None:
        """
        Validate, authorize, and apply one replicated operation payload.
        """
        op_id = str(payload.get("op_id", ""))
        if not op_id:
            self._inc_stat("dropped_messages")
            raise ValueError("operation payload missing op_id")
        if not self._record_operation(op_id):
            return

        if not self._is_acl_allowed(sender_node_id=sender_node_id, payload=payload):
            self._inc_stat("acl_denied")
            self._inc_stat("dropped_messages")
            raise PermissionError("ACL denied for sender operation")

        collection = str(payload.get("collection", ""))
        if self._is_centralized_data_operation(collection):
            # Centralized backends (for example Redis) are globally shared, so
            # replicated map/list/queue payloads do not need local re-apply.
            return
        if collection != "topic":
            self._append_wal_entry(payload)

        if self.config.consensus.enabled:
            self._apply_ordered_remote_payload(payload, sender_node_id=sender_node_id)
        else:
            self._apply_payload(payload, source="remote")
            self._inc_stat("remote_mutations")
            self._schedule_persist()

    def _apply_ordered_remote_payload(self, payload: dict[str, Any], *, sender_node_id: str) -> None:
        """
        Apply remote payload in leader-defined log order.
        """
        term = int(payload.get("term", 0))
        log_index = int(payload.get("log_index", 0))
        if log_index <= 0:
            self._apply_payload(payload, source="remote")
            self._inc_stat("remote_mutations")
            self._schedule_persist()
            return

        with self._consensus_lock:
            if term >= self._term:
                if term > self._term:
                    self._term = term
                    self._voted_for = None
                if sender_node_id:
                    self._leader_id = sender_node_id
                self._last_heartbeat_received = time.monotonic()
            if log_index <= self._last_applied_index:
                return
            self._pending_ordered_ops[log_index] = payload

            while True:
                next_index = self._last_applied_index + 1
                next_payload = self._pending_ordered_ops.pop(next_index, None)
                if next_payload is None:
                    break
                self._apply_payload(next_payload, source="remote")
                self._last_applied_index = next_index
                self._inc_stat("remote_mutations")
                self._schedule_persist()

    def _apply_payload(self, payload: dict[str, Any], *, source: str) -> Any:
        """
        Apply one payload to local in-memory state.
        """
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        name = str(payload.get("name", ""))
        trace_id = str(payload.get("trace_id", ""))

        if collection == "topic":
            if action == "publish":
                self._dispatch_topic(name, payload.get("message"))
                self._trace("topic_publish_applied", trace_id=trace_id, collection=name, source=source)
            return None

        if collection == "cp_lock":
            result = self._apply_cp_lock_payload(payload)
            self._inc_stat("cp_operations")
            self._trace(
                "cp_lock_operation_applied",
                trace_id=trace_id,
                collection=collection,
                name=name,
                action=action,
                source=source,
                log_index=int(payload.get("log_index", 0)),
            )
            return result

        result = self._store.apply_mutation(payload)
        log_index = int(payload.get("log_index", 0))
        if source == "local":
            with self._consensus_lock:
                if log_index > self._last_applied_index:
                    self._last_applied_index = log_index
        self._trace(
            "collection_mutation_applied",
            trace_id=trace_id,
            collection=collection,
            name=name,
            action=action,
            source=source,
            log_index=log_index,
        )
        return result

    def _build_operation_payload(self, operation: dict[str, Any], *, origin_node_id: str) -> dict[str, Any]:
        """
        Build ordered operation payload with term/index/trace metadata.
        """
        with self._consensus_lock:
            if self._term <= 0:
                self._term = 1
            term = self._term
            log_index = self._next_log_index
            self._next_log_index += 1
        trace_id = uuid.uuid4().hex
        payload: dict[str, Any] = {
            "op_id": uuid.uuid4().hex,
            "origin_node_id": origin_node_id,
            "collection": str(operation["collection"]),
            "name": str(operation["name"]),
            "action": str(operation["action"]),
            "term": term,
            "log_index": log_index,
            "trace_id": trace_id,
            "ts_ms": int(time.time() * 1000),
        }
        for key, value in operation.items():
            if key in {"collection", "name", "action"}:
                continue
            payload[key] = value
        return payload

    def _required_ack_count(self, mode: ConsistencyMode) -> int:
        """
        Compute required acknowledgements for selected consistency mode.
        """
        cluster_size = len(self.members()) + 1
        if mode == ConsistencyMode.BEST_EFFORT:
            return 1
        if mode in {ConsistencyMode.QUORUM, ConsistencyMode.LINEARIZABLE}:
            return (cluster_size // 2) + 1
        if mode == ConsistencyMode.ALL:
            return cluster_size
        return 1

    def _replicate_operation_sync(
        self,
        payload: dict[str, Any],
        *,
        required_acks: int,
        timeout_seconds: float,
    ) -> int:
        """
        Replicate one payload synchronously and return successful peer acks.
        """
        peers = self.members()
        if not peers:
            return 0
        successes = 0
        deadline = time.monotonic() + timeout_seconds
        for peer in peers:
            if 1 + successes >= required_acks:
                break
            if time.monotonic() >= deadline:
                break
            remaining = max(0.1, deadline - time.monotonic())
            if self._deliver_single_operation(peer, payload, timeout_seconds=remaining):
                successes += 1
        return successes
