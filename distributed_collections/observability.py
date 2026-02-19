"""
Lightweight HTTP observability endpoint for cluster nodes.

Exposes:

* ``/healthz``: JSON health status
* ``/metrics``: Prometheus-style text metrics
* ``/traces``: recent trace/event records
"""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable


class _ObservabilityHandler(BaseHTTPRequestHandler):
    """HTTP request handler backed by callback providers."""

    server_version = "distributed-collections-observability/1.0"

    def do_GET(self) -> None:  # noqa: N802 - httpserver naming convention
        if self.path == "/healthz":
            payload = self.server.health_provider()  # type: ignore[attr-defined]
            self._send_json(payload, status=HTTPStatus.OK)
            return
        if self.path == "/metrics":
            payload = self.server.metrics_provider()  # type: ignore[attr-defined]
            self._send_text(payload, status=HTTPStatus.OK)
            return
        if self.path == "/traces":
            payload = self.server.traces_provider()  # type: ignore[attr-defined]
            self._send_json(payload, status=HTTPStatus.OK)
            return
        self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003 - stdlib name
        """Silence default HTTP request logging."""
        return

    def _send_json(self, payload: dict[str, Any], *, status: HTTPStatus) -> None:
        data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, payload: str, *, status: HTTPStatus) -> None:
        data = payload.encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class _ObservabilityHTTPServer(HTTPServer):
    """HTTP server carrying callback providers for handler usage."""

    def __init__(
        self,
        server_address: tuple[str, int],
        health_provider: Callable[[], dict[str, Any]],
        metrics_provider: Callable[[], str],
        traces_provider: Callable[[], dict[str, Any]],
    ) -> None:
        super().__init__(server_address, _ObservabilityHandler)
        self.health_provider = health_provider
        self.metrics_provider = metrics_provider
        self.traces_provider = traces_provider


class ObservabilityServer:
    """
    Background HTTP server exposing node health and metrics.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        health_provider: Callable[[], dict[str, Any]],
        metrics_provider: Callable[[], str],
        traces_provider: Callable[[], dict[str, Any]],
    ) -> None:
        self._host = host
        self._port = int(port)
        self._health_provider = health_provider
        self._metrics_provider = metrics_provider
        self._traces_provider = traces_provider
        self._server: _ObservabilityHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background observability HTTP server."""
        if self._thread and self._thread.is_alive():
            return
        self._server = _ObservabilityHTTPServer(
            (self._host, self._port),
            health_provider=self._health_provider,
            metrics_provider=self._metrics_provider,
            traces_provider=self._traces_provider,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.2},
            name="cluster-observability-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Shutdown HTTP server."""
        if not self._server:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread:
            self._thread.join(timeout=1.0)
        self._server = None
        self._thread = None
