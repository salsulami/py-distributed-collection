from __future__ import annotations

import logging
import threading
import time
from typing import Any

from ..config import NodeAddress
from ..exceptions import AuthenticationError, ProtocolVersionError
from ..protocol import MessageKind, assert_authenticated, assert_protocol_compatible

_LOGGER = logging.getLogger(__name__)


class ClusterTransportMixin:
    """
    Transport envelope handling and request dispatch.
    """

    def _handle_transport_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        Process one inbound protocol message and return one response envelope.
        """
        _LOGGER.debug(
            "Inbound transport message kind=%s sender_node_id=%s cluster=%s",
            message.get("kind"),
            message.get("sender_node_id"),
            message.get("cluster"),
        )
        try:
            assert_protocol_compatible(
                message,
                min_supported_version=self.config.upgrade.min_compatible_protocol_version,
                max_supported_version=self.config.upgrade.max_compatible_protocol_version,
            )
        except ProtocolVersionError as exc:
            self._inc_stat("protocol_failures")
            self._inc_stat("dropped_messages")
            _LOGGER.debug("Dropping message due to protocol incompatibility reason=%s", exc)
            return self._make_message(MessageKind.ERROR, {"reason": str(exc)})

        try:
            assert_authenticated(message, self.config.security.shared_token)
        except AuthenticationError as exc:
            self._inc_stat("auth_failures")
            self._inc_stat("dropped_messages")
            _LOGGER.debug("Dropping message due to authentication failure reason=%s", exc)
            return self._make_message(MessageKind.ERROR, {"reason": str(exc)})

        if message.get("cluster") != self.config.cluster_name:
            self._inc_stat("dropped_messages")
            _LOGGER.debug(
                "Dropping message due to cluster mismatch expected=%s actual=%s",
                self.config.cluster_name,
                message.get("cluster"),
            )
            return self._make_message(MessageKind.ERROR, {"reason": "cluster mismatch"})

        kind = str(message.get("kind", ""))
        sender_node_id = str(message.get("sender_node_id", ""))
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            _LOGGER.debug("Normalizing non-dict payload for kind=%s", kind)
            payload = {}

        if kind == MessageKind.HANDSHAKE.value:
            return self._handle_handshake(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.STATE_REQUEST.value:
            _LOGGER.debug("Handling state request from sender_node_id=%s", sender_node_id)
            return self._make_message(
                MessageKind.STATE_RESPONSE,
                {"snapshot": self._snapshot_payload()},
            )
        if kind == MessageKind.OPERATION.value:
            _LOGGER.debug(
                "Handling single replicated operation sender_node_id=%s op_id=%s collection=%s action=%s",
                sender_node_id,
                payload.get("op_id"),
                payload.get("collection"),
                payload.get("action"),
            )
            try:
                self._handle_remote_operation(payload, sender_node_id=sender_node_id)
            except (PermissionError, ValueError) as exc:
                _LOGGER.debug("Rejecting replicated operation reason=%s", exc)
                return self._make_message(MessageKind.ERROR, {"reason": str(exc)})
            return self._make_message(MessageKind.OPERATION, {"status": "ok"})
        if kind == MessageKind.OPERATION_BATCH.value:
            operations = payload.get("operations", [])
            _LOGGER.debug(
                "Handling operation batch sender_node_id=%s operations_count=%d",
                sender_node_id,
                len(operations) if isinstance(operations, list) else 0,
            )
            if isinstance(operations, list):
                try:
                    for item in operations:
                        if isinstance(item, dict):
                            self._handle_remote_operation(item, sender_node_id=sender_node_id)
                except (PermissionError, ValueError) as exc:
                    _LOGGER.debug("Rejecting operation batch reason=%s", exc)
                    return self._make_message(MessageKind.ERROR, {"reason": str(exc)})
            return self._make_message(MessageKind.OPERATION_BATCH, {"status": "ok"})
        if kind == MessageKind.FORWARD_OPERATION.value:
            return self._handle_forwarded_operation(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.VOTE_REQUEST.value:
            return self._handle_vote_request(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.HEARTBEAT.value:
            return self._handle_heartbeat(payload, sender_node_id=sender_node_id)
        _LOGGER.debug("Received unsupported message kind=%s", kind)
        return self._make_message(
            MessageKind.ERROR,
            {"reason": f"unsupported message kind: {kind!r}"},
        )

    def _handle_handshake(self, payload: dict[str, Any], *, sender_node_id: str) -> dict[str, Any]:
        """
        Register handshake peer and return known cluster members.
        """
        peer_min = int(payload.get("protocol_min", 1))
        peer_max = int(payload.get("protocol_max", 1))
        _LOGGER.debug(
            "Handling handshake sender_node_id=%s peer_protocol_range=[%d,%d]",
            sender_node_id,
            peer_min,
            peer_max,
        )
        if not self._protocol_ranges_overlap(peer_min, peer_max):
            _LOGGER.debug(
                "Rejecting handshake due to protocol range mismatch sender_node_id=%s",
                sender_node_id,
            )
            return self._make_message(
                MessageKind.ERROR,
                {"reason": "protocol compatibility range mismatch"},
            )

        raw_address = payload.get("address")
        if isinstance(raw_address, dict):
            try:
                address = NodeAddress.from_dict(raw_address)
                node_id = str(payload.get("node_id", "")) or sender_node_id or None
                self._add_member(address, node_id=node_id)
                _LOGGER.debug(
                    "Registered handshake member node_id=%s host=%s port=%d",
                    node_id,
                    address.host,
                    address.port,
                )
            except (KeyError, TypeError, ValueError):
                pass

        members_payload = self._member_nodes_payload()
        _LOGGER.debug(
            "Handshake ack prepared local_node_id=%s member_payload_count=%d",
            self.node_id,
            len(members_payload),
        )
        return self._make_message(
            MessageKind.HANDSHAKE_ACK,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "members": members_payload,
                "protocol_min": self.config.upgrade.min_compatible_protocol_version,
                "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            },
        )

    def _handle_forwarded_operation(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Execute a follower-forwarded write operation on leader.
        """
        _LOGGER.debug(
            "Handling forwarded operation sender_node_id=%s is_leader=%s",
            sender_node_id,
            self._is_leader(),
        )
        if self.config.consensus.enabled and not self._is_leader():
            _LOGGER.debug("Rejecting forwarded operation because local node is not leader")
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {
                    "status": "error",
                    "reason": "not leader",
                    "leader_address": (
                        self._leader_address.as_dict() if self._leader_address is not None else None
                    ),
                },
            )
        if (
            self.config.consensus.enabled
            and self.config.consensus.require_majority_for_writes
            and not self._leader_lease_valid()
        ):
            _LOGGER.debug("Rejecting forwarded operation due to expired leader lease")
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "leader quorum lease expired"},
            )
        operation = payload.get("operation")
        if not isinstance(operation, dict):
            _LOGGER.debug("Rejecting forwarded operation due to missing payload")
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "missing operation payload"},
            )
        if not self._is_acl_allowed(sender_node_id=sender_node_id, payload=operation):
            self._inc_stat("acl_denied")
            _LOGGER.debug(
                "Rejecting forwarded operation due to ACL sender_node_id=%s collection=%s action=%s",
                sender_node_id,
                operation.get("collection"),
                operation.get("action"),
            )
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "ACL denied for forwarded operation"},
            )
        try:
            result = self._execute_leader_operation(operation, origin_node_id=sender_node_id or self.node_id)
            _LOGGER.debug(
                "Forwarded operation applied sender_node_id=%s collection=%s action=%s",
                sender_node_id,
                operation.get("collection"),
                operation.get("action"),
            )
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "ok", "result": result},
            )
        except Exception as exc:  # noqa: BLE001 - forwarded operation returns structured errors
            _LOGGER.debug("Forwarded operation failed reason=%s", exc)
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": str(exc)},
            )

    def _handle_vote_request(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Process one leader-election vote request.
        """
        candidate_id = str(payload.get("candidate_id", ""))
        request_term = int(payload.get("term", 0))
        candidate_last_index = int(payload.get("last_log_index", 0))
        granted = False
        _LOGGER.debug(
            "Handling vote request candidate_id=%s sender_node_id=%s term=%d last_log_index=%d",
            candidate_id,
            sender_node_id,
            request_term,
            candidate_last_index,
        )

        if sender_node_id and sender_node_id == candidate_id:
            raw = payload.get("address")
            if isinstance(raw, dict):
                try:
                    self._add_member(NodeAddress.from_dict(raw), node_id=candidate_id)
                except (TypeError, ValueError, KeyError):
                    pass

        with self._consensus_lock:
            if request_term < self._term:
                granted = False
            else:
                if request_term > self._term:
                    self._term = request_term
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_address = None
                up_to_date = candidate_last_index >= self._last_applied_index
                if up_to_date and (self._voted_for is None or self._voted_for == candidate_id):
                    self._voted_for = candidate_id
                    self._last_heartbeat_received = time.monotonic()
                    granted = True
        _LOGGER.debug(
            "Vote response candidate_id=%s granted=%s local_term=%d",
            candidate_id,
            granted,
            self._term,
        )

        return self._make_message(
            MessageKind.VOTE_RESPONSE,
            {
                "term": self._term,
                "granted": granted,
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )

    def _handle_heartbeat(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Process one leader heartbeat and refresh lease/failure-detector state.
        """
        leader_term = int(payload.get("term", 0))
        leader_id = str(payload.get("leader_id", sender_node_id or ""))
        _LOGGER.debug(
            "Handling heartbeat leader_id=%s sender_node_id=%s leader_term=%d",
            leader_id,
            sender_node_id,
            leader_term,
        )
        raw_address = payload.get("address")
        leader_address: NodeAddress | None = None
        if isinstance(raw_address, dict):
            try:
                leader_address = NodeAddress.from_dict(raw_address)
            except (TypeError, ValueError, KeyError):
                leader_address = None

        leader_changed = False
        with self._consensus_lock:
            if leader_term >= self._term:
                if leader_term > self._term:
                    self._term = leader_term
                    self._voted_for = None
                if leader_id:
                    leader_changed = leader_id != self._leader_id
                    self._leader_id = leader_id
                if leader_address is not None:
                    self._leader_address = leader_address
                self._last_heartbeat_received = time.monotonic()

        if leader_id and leader_address is not None:
            self._add_member(leader_address, node_id=leader_id)

        # Partition-healing bootstrap: on leader change, re-sync state snapshot.
        if leader_changed and leader_address is not None and self.config.auto_sync_on_join:
            _LOGGER.debug(
                "Leader changed to node_id=%s host=%s port=%d triggering resync",
                leader_id,
                leader_address.host,
                leader_address.port,
            )
            threading.Thread(
                target=self._sync_from_peer_list,
                args=([leader_address],),
                name="cluster-resync-on-leader-change",
                daemon=True,
            ).start()
        _LOGGER.debug(
            "Heartbeat ack prepared local_term=%d leader_id=%s leader_changed=%s",
            self._term,
            leader_id,
            leader_changed,
        )

        return self._make_message(
            MessageKind.HEARTBEAT_ACK,
            {
                "term": self._term,
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )
