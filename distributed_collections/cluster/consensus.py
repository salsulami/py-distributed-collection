from __future__ import annotations

import random
import threading
import time

from ..config import NodeAddress
from ..protocol import MessageKind
from ..transport import request_response


class ClusterConsensusMixin:
    """
    Leader election and quorum lease control.
    """

    def _start_consensus_worker(self) -> None:
        """Start election/heartbeat worker when consensus is enabled."""
        if not self.config.consensus.enabled:
            return
        self._consensus_stop.clear()
        self._consensus_thread = threading.Thread(
            target=self._consensus_loop,
            name="cluster-consensus-worker",
            daemon=True,
        )
        self._consensus_thread.start()

    def _stop_consensus_worker(self) -> None:
        """Stop consensus worker."""
        if not self.config.consensus.enabled:
            return
        self._consensus_stop.set()
        if self._consensus_thread:
            self._consensus_thread.join(timeout=1.0)
            self._consensus_thread = None

    def _consensus_loop(self) -> None:
        """
        Leader heartbeat and election loop.
        """
        next_heartbeat = time.monotonic()
        while not self._consensus_stop.is_set():
            now = time.monotonic()
            if self._is_leader():
                if now >= next_heartbeat:
                    self._send_heartbeats()
                    next_heartbeat = now + self.config.consensus.heartbeat_interval_seconds
                time.sleep(0.05)
                continue

            timeout = self.config.consensus.election_timeout_seconds * random.uniform(0.9, 1.3)
            with self._consensus_lock:
                elapsed = now - self._last_heartbeat_received
            if elapsed >= timeout:
                self._start_election()
            time.sleep(0.05)

    def _start_election(self) -> None:
        """
        Start one vote round and become leader if quorum is reached.
        """
        self._inc_stat("leader_elections_started")
        with self._consensus_lock:
            self._term += 1
            term = self._term
            self._voted_for = self.node_id
            self._leader_id = None
            self._leader_address = None
            last_log_index = self._last_applied_index

        votes = 1
        for peer in self.members():
            request = self._make_message(
                MessageKind.VOTE_REQUEST,
                {
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "address": self.config.advertise_address.as_dict(),
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
                continue

            if response.get("kind") != MessageKind.VOTE_RESPONSE.value:
                continue
            payload = response.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if int(payload.get("term", 0)) > term:
                with self._consensus_lock:
                    self._term = int(payload["term"])
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_address = None
                return
            if bool(payload.get("granted")):
                votes += 1
            remote_node_id = payload.get("node_id")
            raw_addr = payload.get("address")
            if isinstance(remote_node_id, str) and isinstance(raw_addr, dict):
                try:
                    self._add_member(NodeAddress.from_dict(raw_addr), node_id=remote_node_id)
                except (TypeError, ValueError, KeyError):
                    pass

        if votes >= self._quorum_size():
            with self._consensus_lock:
                self._leader_id = self.node_id
                self._leader_address = self.config.advertise_address
                self._last_heartbeat_received = time.monotonic()
                self._last_majority_contact = time.monotonic()
            self._inc_stat("leader_elections_won")
            self._trace("election_won", term=term, votes=votes)
        else:
            with self._consensus_lock:
                self._last_heartbeat_received = time.monotonic()
            self._trace("election_lost", term=term, votes=votes)

    def _send_heartbeats(self) -> None:
        """
        Send leader heartbeats and update quorum lease state.
        """
        if not self._is_leader():
            return
        self._inc_stat("leader_heartbeats_sent")
        with self._consensus_lock:
            term = self._term
            last_applied = self._last_applied_index
        acknowledged = 1

        heartbeat = self._make_message(
            MessageKind.HEARTBEAT,
            {
                "term": term,
                "leader_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "last_applied_index": last_applied,
            },
        )
        for peer in self.members():
            try:
                response = request_response(
                    peer,
                    heartbeat,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                    ssl_context=self._client_ssl_context,
                    server_hostname=self.config.tls.server_hostname,
                    min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                    max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
                )
            except Exception:
                self._record_member_failure(peer)
                continue
            if response.get("kind") != MessageKind.HEARTBEAT_ACK.value:
                continue
            payload = response.get("payload", {})
            if isinstance(payload, dict):
                remote_node_id = payload.get("node_id")
                raw_addr = payload.get("address")
                if isinstance(remote_node_id, str) and isinstance(raw_addr, dict):
                    try:
                        self._add_member(NodeAddress.from_dict(raw_addr), node_id=remote_node_id)
                    except (TypeError, ValueError, KeyError):
                        pass
            acknowledged += 1
            self._clear_member_failure(peer)
            self._inc_stat("leader_heartbeats_acked")

        if acknowledged >= self._quorum_size():
            with self._consensus_lock:
                self._last_majority_contact = time.monotonic()

    def _is_leader(self) -> bool:
        """Return true when this node is currently leader."""
        if not self.config.consensus.enabled:
            return True
        with self._consensus_lock:
            return self._leader_id == self.node_id

    def _leader_lease_valid(self) -> bool:
        """Return true when leader still has quorum lease."""
        if not self.config.consensus.enabled:
            return True
        with self._consensus_lock:
            last_contact = self._last_majority_contact
        return (time.monotonic() - last_contact) <= self.config.consensus.leader_lease_seconds

    def _quorum_size(self) -> int:
        """Compute current majority quorum size."""
        cluster_size = len(self.members()) + 1
        return (cluster_size // 2) + 1

    def _resolve_leader_address(self) -> NodeAddress | None:
        """
        Resolve current leader endpoint for forwarded writes.
        """
        if self._is_leader():
            return self.config.advertise_address
        with self._consensus_lock:
            if self._leader_address is not None:
                return self._leader_address
            if self._leader_id is not None:
                return self._member_nodes.get(self._leader_id)
        return None
