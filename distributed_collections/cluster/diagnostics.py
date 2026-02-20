from __future__ import annotations

from typing import Any


class ClusterDiagnosticsMixin:
    """
    Public diagnostics and metrics helpers.
    """

    def stats(self) -> dict[str, Any]:
        """
        Return cumulative runtime counters and live gauges.
        """
        with self._stats_lock:
            payload = dict(self._stats)
        payload["member_count"] = len(self.members())
        payload["replication_queue_depth"] = self._replication_queue.qsize()
        payload["running"] = self.is_running
        payload["leader"] = self._leader_id
        payload["is_leader"] = self._is_leader()
        payload["term"] = self._term
        payload["last_applied_index"] = self._last_applied_index
        payload["cp_lock_count"] = self._cp_lock_count()
        return payload

    def health(self) -> dict[str, Any]:
        """
        Return structured health state for readiness/liveness checks.
        """
        status = "ok"
        if not self.is_running:
            status = "stopped"
        elif self._is_leader() and self.config.consensus.require_majority_for_writes:
            if not self._leader_lease_valid():
                status = "degraded"
        return {
            "status": status,
            "cluster": self.config.cluster_name,
            "node_id": self.node_id,
            "leader_id": self._leader_id,
            "is_leader": self._is_leader(),
            "member_count": len(self.members()),
            "term": self._term,
            "last_applied_index": self._last_applied_index,
            "protocol_min": self.config.upgrade.min_compatible_protocol_version,
            "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            "protocol_local": self.config.upgrade.max_compatible_protocol_version,
        }

    def metrics_text(self) -> str:
        """
        Return Prometheus-style metrics payload as text.
        """
        stats = self.stats()
        lines = []
        for key, value in sorted(stats.items()):
            if isinstance(value, bool):
                numeric = 1 if value else 0
                lines.append(f"distributed_collections_{key} {numeric}")
            elif isinstance(value, (int, float)):
                lines.append(f"distributed_collections_{key} {value}")
        return "\n".join(lines) + "\n"

    def recent_traces(self) -> dict[str, Any]:
        """
        Return bounded in-memory trace event history.
        """
        with self._trace_lock:
            traces = list(self._trace_history)
        return {"traces": traces}
