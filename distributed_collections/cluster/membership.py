from __future__ import annotations

import logging
import time
from typing import Any

from ..config import DiscoveryMode, NodeAddress
from ..discovery import (
    discover_multicast_peers,
    discover_static_peers,
    merge_discovery_results,
)

_LOGGER = logging.getLogger(__name__)


class ClusterMembershipMixin:
    """
    Discovery and membership state helpers.
    """

    def join_cluster(self) -> list[NodeAddress]:
        """
        Discover peers, perform handshakes, and optionally synchronize state.
        """
        self._ensure_running()

        discovered_groups: list[list[NodeAddress]] = []
        if DiscoveryMode.STATIC in self.config.enabled_discovery:
            discovered_groups.append(discover_static_peers(self.config))
        if DiscoveryMode.MULTICAST in self.config.enabled_discovery:
            discovered_groups.append(
                discover_multicast_peers(
                    config=self.config,
                    node_id=self.node_id,
                    socket_timeout_seconds=self.config.socket_timeout_seconds,
                )
            )
        discovered = merge_discovery_results(discovered_groups)
        reachable_peers: list[NodeAddress] = []

        for peer in discovered:
            payload = self._handshake_peer(peer)
            if payload is None:
                continue
            reachable_peers.append(peer)

            remote_node_id = payload.get("node_id")
            if isinstance(remote_node_id, str):
                self._add_member(peer, node_id=remote_node_id)
            else:
                self._add_member(peer)

            for member_node_id, member_address in self._member_nodes_from_payload(
                payload.get("members", [])
            ):
                self._add_member(member_address, node_id=member_node_id)

        if self.config.auto_sync_on_join and reachable_peers and not self._store_is_centralized:
            self._sync_from_peer_list(reachable_peers)
        self._trace("join_completed", discovered=len(discovered), reachable=len(reachable_peers))
        return discovered

    def members(self) -> list[NodeAddress]:
        """
        Return known remote member addresses sorted by host/port.
        """
        with self._members_lock:
            return sorted(self._members, key=lambda item: (item.host, item.port))

    def wait_for_next_join_window(self) -> None:
        """Sleep for configured reconnect interval."""
        time.sleep(float(self.config.reconnect_interval_seconds))

    def _add_member(self, address: NodeAddress, *, node_id: str | None = None) -> None:
        """
        Add/update one member endpoint and optional node-id mapping.
        """
        if address == self.config.advertise_address:
            if node_id:
                with self._members_lock:
                    self._member_nodes[node_id] = address
            return
        remote_count = 0
        is_new_member = False
        with self._members_lock:
            is_new_member = address not in self._members
            self._members.add(address)
            remote_count = len(self._members)
            if node_id:
                self._member_nodes[node_id] = address
        with self._member_failures_lock:
            self._member_failures.pop(address, None)
        if is_new_member:
            member_label = node_id if node_id else "unknown"
            this_node = self.config.advertise_address
            _LOGGER.info(
                "Member joined cluster: cluster=%s this=%s(%s)(this) node_id=%s host=%s port=%s remote_members=%d",
                self.config.cluster_name,
                this_node.host,
                this_node.port,
                member_label,
                address.host,
                address.port,
                remote_count,
            )
            self._trace(
                "member_joined",
                member_node_id=member_label,
                member_address=address.as_dict(),
                remote_member_count=remote_count,
            )
        if remote_count > 0 and self.config.consensus.require_majority_for_writes:
            # Force immediate lease refresh when cluster size changes.
            with self._consensus_lock:
                if self._leader_id == self.node_id:
                    self._last_majority_contact = 0.0

    def _record_member_failure(self, peer: NodeAddress) -> None:
        """
        Track peer failures and evict when threshold is exceeded.
        """
        evict = False
        with self._member_failures_lock:
            count = self._member_failures.get(peer, 0) + 1
            self._member_failures[peer] = count
            if count >= self.config.replication.member_failure_threshold:
                self._member_failures.pop(peer, None)
                evict = True
        if evict:
            stale_nodes: list[str] = []
            with self._members_lock:
                self._members.discard(peer)
                stale_nodes = [node_id for node_id, addr in self._member_nodes.items() if addr == peer]
                for node_id in stale_nodes:
                    self._member_nodes.pop(node_id, None)
            self._inc_stat("members_evicted")
            members_view, total_members = self._cluster_members_log_view()
            member_node_id = ",".join(stale_nodes) if stale_nodes else "unknown"
            _LOGGER.info(
                "Member left cluster: cluster=%s node_id=%s host=%s port=%s heartbeat_failures=%d members=%s total_members=%d",
                self.config.cluster_name,
                member_node_id,
                peer.host,
                peer.port,
                count,
                members_view,
                total_members,
            )
            self._trace(
                "member_left",
                member_node_id=member_node_id,
                member_address=peer.as_dict(),
                heartbeat_failures=count,
                members=members_view,
                total_members=total_members,
            )

    def _clear_member_failure(self, peer: NodeAddress) -> None:
        """Clear peer failure counter after successful delivery."""
        with self._member_failures_lock:
            self._member_failures.pop(peer, None)

    def _member_nodes_payload(self) -> list[dict[str, Any]]:
        """
        Return known node-id/address tuples for handshake metadata.
        """
        with self._members_lock:
            items = list(self._member_nodes.items())
        payload = []
        seen = set()
        for node_id, address in items:
            key = (node_id, address.host, address.port)
            if key in seen:
                continue
            seen.add(key)
            payload.append({"node_id": node_id, "address": address.as_dict()})
        if self.node_id not in {item["node_id"] for item in payload}:
            payload.append(
                {
                    "node_id": self.node_id,
                    "address": self.config.advertise_address.as_dict(),
                }
            )
        return payload

    def _member_nodes_from_payload(
        self,
        raw_members: Any,
    ) -> list[tuple[str | None, NodeAddress]]:
        """
        Parse handshake member list with backward compatibility.
        """
        if not isinstance(raw_members, list):
            return []
        parsed: list[tuple[str | None, NodeAddress]] = []
        for raw in raw_members:
            if not isinstance(raw, dict):
                continue
            if "address" in raw:
                node_id = raw.get("node_id")
                raw_address = raw.get("address")
                if isinstance(raw_address, dict):
                    try:
                        parsed.append(
                            (
                                str(node_id) if isinstance(node_id, str) and node_id else None,
                                NodeAddress.from_dict(raw_address),
                            )
                        )
                    except (TypeError, ValueError, KeyError):
                        continue
                continue
            # Backward compatibility: plain address object.
            try:
                parsed.append((None, NodeAddress.from_dict(raw)))
            except (TypeError, ValueError, KeyError):
                continue
        return parsed
