from __future__ import annotations

import time

from ..observability import ObservabilityServer


class ClusterObservabilityMixin:
    """
    Observability server lifecycle and trace history.
    """

    def _start_observability_server(self) -> None:
        """Start HTTP observability server when enabled."""
        if not self.config.observability.enable_http:
            return
        try:
            self._observability_server = ObservabilityServer(
                host=self.config.observability.host,
                port=self.config.observability.port,
                health_provider=self.health,
                metrics_provider=self.metrics_text,
                traces_provider=self.recent_traces,
            )
            self._observability_server.start()
        except Exception as exc:
            self._observability_server = None
            self._trace("observability_start_failed", reason=str(exc))

    def _stop_observability_server(self) -> None:
        """Stop observability server when running."""
        if self._observability_server is None:
            return
        self._observability_server.stop()
        self._observability_server = None

    def _trace(self, event: str, *, trace_id: str | None = None, **details: object) -> None:
        """
        Record one structured trace event in bounded memory history.
        """
        if not self.config.observability.enable_tracing:
            return
        entry = {
            "ts_ms": int(time.time() * 1000),
            "event": event,
            "node_id": self.node_id,
        }
        if trace_id:
            entry["trace_id"] = trace_id
        if details:
            entry["details"] = details
        with self._trace_lock:
            self._trace_history.append(entry)
