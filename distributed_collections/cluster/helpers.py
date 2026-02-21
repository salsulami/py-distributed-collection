from __future__ import annotations

from typing import Any

from ..config import ConsistencyMode
from ..exceptions import ClusterNotRunningError
from ..protocol import MessageKind, make_message


class ClusterHelperMixin:
    """
    Low-level utility methods shared across mixins.
    """

    def _make_message(self, kind: MessageKind, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Build signed protocol envelope using local identity/security settings.
        """
        return make_message(
            kind,
            self.config.cluster_name,
            payload,
            sender_node_id=self.node_id,
            security_token=self.config.security.shared_token,
        )

    def _ensure_running(self) -> None:
        if not self.is_running:
            raise ClusterNotRunningError(
                "Cluster node is not running. Call start() before using distributed collections."
            )

    def _is_centralized_data_operation(self, collection: str) -> bool:
        """
        Return true when operation targets centralized backend state.

        Topic traffic is excluded because it is transient pub/sub fan-out, not
        persistent collection storage.
        """
        return self._store_is_centralized and collection in {"map", "list", "queue"}

    def _is_cp_operation(self, collection: str) -> bool:
        """Return true when operation belongs to the CP coordination subsystem."""
        return collection.startswith("cp_")

    def _effective_consistency_mode(self, collection: str) -> ConsistencyMode:
        """
        Resolve effective consistency mode for one collection operation.

        CP operations and (optionally) core collection mutations are always
        promoted to linearizable commits.
        """
        if self._is_cp_operation(collection):
            return ConsistencyMode.LINEARIZABLE
        if (
            self.config.cp.enabled
            and self.config.cp.mandatory_for_core_mutations
            and collection in {"map", "list", "queue", "topic"}
        ):
            return ConsistencyMode.LINEARIZABLE
        return self.config.consistency.mode

    def _cp_lock_count(self) -> int:
        """Return number of currently tracked CP locks."""
        with self._cp_state_lock:
            return len(self._cp_locks)

    def _inc_stat(self, key: str, *, delta: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + delta

    def _record_operation(self, operation_id: str) -> bool:
        """
        Record operation ID and return false when already seen.
        """
        with self._seen_lock:
            if operation_id in self._seen_operation_ids:
                return False
            self._seen_operation_ids.add(operation_id)
            self._seen_operation_order.append(operation_id)
            if len(self._seen_operation_order) > self._seen_operation_limit:
                expired = self._seen_operation_order.popleft()
                self._seen_operation_ids.discard(expired)
            return True

    def _cluster_members_log_view(self) -> tuple[str, int]:
        """
        Build one-line cluster-member snapshot for logs.

        Returns a tuple of ``([ip(port),...], total_members)`` where total includes
        the local node.
        """
        with self._members_lock:
            remote_members = sorted(self._members, key=lambda item: (item.host, item.port))
        local = self.config.advertise_address
        members = [local]
        members.extend(member for member in remote_members if member != local)
        formatted = ",".join(
            f"{member.host}({member.port})(this)" if member == local else f"{member.host}({member.port})"
            for member in members
        )
        return f"[{formatted}]", len(members)
