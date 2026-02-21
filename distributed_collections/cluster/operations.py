from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from ..config import ConsistencyMode
from ..exceptions import ClusterNotRunningError
from ..protocol import MessageKind
from ..transport import request_response

_LOGGER = logging.getLogger(__name__)


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
            _LOGGER.debug(
                "Rejecting local operation because CP subsystem is disabled collection=%s name=%s action=%s",
                collection,
                name,
                action,
            )
            raise ClusterNotRunningError("CP subsystem is disabled.")

        mode = self._effective_consistency_mode(collection)
        _LOGGER.debug(
            "Submitting local operation collection=%s name=%s action=%s mode=%s consensus_enabled=%s",
            collection,
            name,
            action,
            mode.value if hasattr(mode, "value") else mode,
            self.config.consensus.enabled,
        )
        if self.config.consensus.enabled and mode != ConsistencyMode.BEST_EFFORT:
            if not self._is_leader():
                _LOGGER.debug(
                    "Forwarding local operation to leader collection=%s name=%s action=%s",
                    collection,
                    name,
                    action,
                )
                return self._forward_operation_to_leader(operation)
            if self.config.consensus.require_majority_for_writes and not self._leader_lease_valid():
                self._inc_stat("quorum_write_failures")
                _LOGGER.debug(
                    "Rejecting local operation due to expired leader lease collection=%s name=%s action=%s",
                    collection,
                    name,
                    action,
                )
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
        operation = self._augment_operation_with_ttl(operation)
        payload = self._build_operation_payload(operation, origin_node_id=origin_node_id)
        op_id = str(payload["op_id"])
        _LOGGER.debug(
            "Executing leader operation op_id=%s origin_node_id=%s collection=%s name=%s action=%s log_index=%s",
            op_id,
            origin_node_id,
            payload.get("collection"),
            payload.get("name"),
            payload.get("action"),
            payload.get("log_index"),
        )
        if not self._record_operation(op_id):
            _LOGGER.debug("Skipping duplicate leader operation op_id=%s", op_id)
            return None

        collection_name = str(payload.get("collection", ""))
        self._append_wal_entry(payload)
        result = self._apply_payload(payload, source="local")
        self._inc_stat("local_mutations")
        self._schedule_persist()

        mode = self._effective_consistency_mode(collection_name)
        if self._is_centralized_data_operation(collection_name):
            _LOGGER.debug(
                "Skipping peer replication for centralized backend op_id=%s collection=%s",
                op_id,
                collection_name,
            )
            return result

        # Commit strategy by consistency mode.
        required = self._required_ack_count(mode)
        if mode == ConsistencyMode.BEST_EFFORT:
            self._enqueue_replication(payload)
            _LOGGER.debug(
                "Best-effort operation queued for async replication op_id=%s collection=%s",
                op_id,
                collection_name,
            )
            return result

        acks = 1 + self._replicate_operation_sync(
            payload,
            required_acks=required,
            timeout_seconds=self.config.consistency.write_timeout_seconds,
        )
        self._enqueue_replication(payload)
        _LOGGER.debug(
            "Synchronous replication completed op_id=%s required_acks=%d received_acks=%d",
            op_id,
            required,
            acks,
        )
        if acks < required:
            self._inc_stat("quorum_write_failures")
            _LOGGER.debug(
                "Write commit failed due to insufficient acknowledgements op_id=%s required=%d received=%d",
                op_id,
                required,
                acks,
            )
            raise ClusterNotRunningError(
                f"Write commit failed; acknowledgements {acks} below required {required}."
            )
        return result

    def _forward_operation_to_leader(self, operation: dict[str, Any]) -> Any:
        """
        Forward write request to active leader for linearizable commit.
        """
        _LOGGER.debug(
            "Resolving leader for forwarded operation collection=%s name=%s action=%s",
            operation.get("collection"),
            operation.get("name"),
            operation.get("action"),
        )
        leader = self._resolve_leader_address()
        if leader is None and self.config.consensus.enabled:
            _LOGGER.debug("No known leader, triggering election before forwarding operation")
            self._start_election()
            leader = self._resolve_leader_address()
            if leader == self.config.advertise_address:
                _LOGGER.debug("Local node became leader while forwarding operation")
                return self._execute_leader_operation(operation, origin_node_id=self.node_id)
        if leader is None:
            self._inc_stat("quorum_write_failures")
            _LOGGER.debug("Forwarded operation failed because no leader is known")
            raise ClusterNotRunningError("No known leader to forward operation.")
        _LOGGER.debug("Forwarding operation to leader host=%s port=%d", leader.host, leader.port)

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
            _LOGGER.debug("Forwarded operation received unexpected response kind=%s", response.get("kind"))
            raise ClusterNotRunningError("Leader returned unexpected forwarded-write response.")
        payload = response.get("payload", {})
        if not isinstance(payload, dict):
            self._inc_stat("quorum_write_failures")
            _LOGGER.debug("Forwarded operation received malformed response payload")
            raise ClusterNotRunningError("Leader returned malformed forwarded-write payload.")
        if payload.get("status") != "ok":
            self._inc_stat("quorum_write_failures")
            _LOGGER.debug("Forwarded operation rejected by leader reason=%s", payload.get("reason"))
            raise ClusterNotRunningError(str(payload.get("reason", "forwarded write rejected")))
        self._inc_stat("forwarded_writes")
        _LOGGER.debug("Forwarded operation acknowledged by leader")
        return payload.get("result")

    def _handle_remote_operation(self, payload: dict[str, Any], *, sender_node_id: str) -> None:
        """
        Validate, authorize, and apply one replicated operation payload.
        """
        op_id = str(payload.get("op_id", ""))
        _LOGGER.debug(
            "Handling remote operation sender_node_id=%s op_id=%s collection=%s name=%s action=%s",
            sender_node_id,
            op_id,
            payload.get("collection"),
            payload.get("name"),
            payload.get("action"),
        )
        if not op_id:
            self._inc_stat("dropped_messages")
            raise ValueError("operation payload missing op_id")
        if not self._record_operation(op_id):
            _LOGGER.debug("Skipping duplicate remote operation op_id=%s", op_id)
            return

        if not self._is_acl_allowed(sender_node_id=sender_node_id, payload=payload):
            self._inc_stat("acl_denied")
            self._inc_stat("dropped_messages")
            _LOGGER.debug(
                "Remote operation denied by ACL sender_node_id=%s op_id=%s",
                sender_node_id,
                op_id,
            )
            raise PermissionError("ACL denied for sender operation")

        collection = str(payload.get("collection", ""))
        if self._is_centralized_data_operation(collection):
            # Centralized backends (for example Redis) are globally shared, so
            # replicated map/list/queue payloads do not need local re-apply.
            _LOGGER.debug(
                "Skipping local apply for centralized backend remote op_id=%s collection=%s",
                op_id,
                collection,
            )
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
        _LOGGER.debug(
            "Applying ordered remote payload sender_node_id=%s term=%d log_index=%d",
            sender_node_id,
            term,
            log_index,
        )
        if log_index <= 0:
            _LOGGER.debug("Applying remote payload without ordered index log_index=%d", log_index)
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
                _LOGGER.debug(
                    "Skipping already applied ordered payload log_index=%d last_applied=%d",
                    log_index,
                    self._last_applied_index,
                )
                return
            self._pending_ordered_ops[log_index] = payload
            _LOGGER.debug(
                "Queued ordered payload log_index=%d pending_count=%d",
                log_index,
                len(self._pending_ordered_ops),
            )

            while True:
                next_index = self._last_applied_index + 1
                next_payload = self._pending_ordered_ops.pop(next_index, None)
                if next_payload is None:
                    break
                self._apply_payload(next_payload, source="remote")
                self._last_applied_index = next_index
                self._inc_stat("remote_mutations")
                self._schedule_persist()
                _LOGGER.debug("Applied ordered payload log_index=%d", next_index)

    def _apply_payload(self, payload: dict[str, Any], *, source: str) -> Any:
        """
        Apply one payload to local in-memory state.
        """
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        name = str(payload.get("name", ""))
        trace_id = str(payload.get("trace_id", ""))
        _LOGGER.debug(
            "Applying payload source=%s collection=%s name=%s action=%s op_id=%s",
            source,
            collection,
            name,
            action,
            payload.get("op_id"),
        )

        if collection == "topic":
            if action == "publish":
                self._dispatch_topic(name, payload.get("message"))
                self._trace("topic_publish_applied", trace_id=trace_id, collection=name, source=source)
                _LOGGER.debug("Applied topic publish source=%s topic=%s", source, name)
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
            _LOGGER.debug("Applied CP lock operation source=%s name=%s action=%s", source, name, action)
            return result

        result: Any
        if action in {"expire_key", "expire_index"} and not self._ttl_eviction_guard_allows(payload):
            result = {"removed": False}
            _LOGGER.debug(
                "Skipped TTL eviction due to guard collection=%s name=%s action=%s",
                collection,
                name,
                action,
            )
        else:
            result = self._store.apply_mutation(payload)
        self._apply_ttl_metadata(
            collection=collection,
            name=name,
            action=action,
            payload=payload,
            result=result,
        )
        self._emit_collection_events_for_mutation(
            collection=collection,
            name=name,
            action=action,
            payload=payload,
            result=result,
            source=source,
        )
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
        _LOGGER.debug(
            "Applied collection mutation source=%s collection=%s name=%s action=%s log_index=%d",
            source,
            collection,
            name,
            action,
            log_index,
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
        _LOGGER.debug(
            "Built operation payload op_id=%s origin_node_id=%s collection=%s name=%s action=%s term=%s log_index=%s",
            payload["op_id"],
            origin_node_id,
            payload["collection"],
            payload["name"],
            payload["action"],
            payload["term"],
            payload["log_index"],
        )
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
            _LOGGER.debug("Synchronous replication skipped because there are no peers")
            return 0
        successes = 0
        deadline = time.monotonic() + timeout_seconds
        _LOGGER.debug(
            "Starting synchronous replication peers=%d required_acks=%d timeout_seconds=%.3f op_id=%s",
            len(peers),
            required_acks,
            timeout_seconds,
            payload.get("op_id"),
        )
        for peer in peers:
            if 1 + successes >= required_acks:
                break
            if time.monotonic() >= deadline:
                _LOGGER.debug("Synchronous replication deadline reached successes=%d", successes)
                break
            remaining = max(0.1, deadline - time.monotonic())
            if self._deliver_single_operation(peer, payload, timeout_seconds=remaining):
                successes += 1
                _LOGGER.debug(
                    "Peer acknowledged synchronous replication host=%s port=%d successes=%d",
                    peer.host,
                    peer.port,
                    successes,
                )
            else:
                _LOGGER.debug(
                    "Peer failed synchronous replication host=%s port=%d",
                    peer.host,
                    peer.port,
                )
        _LOGGER.debug("Synchronous replication finished successes=%d", successes)
        return successes
