"""
TCP transport layer for peer-to-peer cluster messaging.

The transport module provides two focused primitives:

* :class:`TcpTransportServer` for inbound message handling.
* :func:`request_response` for outbound request/response exchange.

Protocol framing and JSON encoding are delegated to :mod:`protocol`.
"""

from __future__ import annotations

import socket
import socketserver
import threading
from typing import Any, Callable

from .config import NodeAddress
from .protocol import recv_frame, send_frame

MessageHandler = Callable[[dict[str, Any]], dict[str, Any]]


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Thread-per-connection TCP server with safe address reuse."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_cls: type[socketserver.BaseRequestHandler],
        message_handler: MessageHandler,
    ) -> None:
        super().__init__(server_address, handler_cls)
        self.message_handler = message_handler


class _ClusterRequestHandler(socketserver.BaseRequestHandler):
    """Handle one inbound framed message and send one response."""

    def handle(self) -> None:
        sock = self.request
        if not isinstance(sock, socket.socket):
            return
        response: dict[str, Any]
        try:
            incoming = recv_frame(sock)
            response = self.server.message_handler(incoming)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - transport must not crash the server loop
            response = {"kind": "error", "cluster": "", "payload": {"reason": str(exc)}}
        send_frame(sock, response)


class TcpTransportServer:
    """
    Host-side TCP server that dispatches cluster messages to a callback.

    Parameters
    ----------
    bind:
        Host/port pair for the listening socket.
    message_handler:
        Callback that receives decoded message dictionaries and returns a
        response dictionary to be sent back to the client.
    """

    def __init__(self, *, bind: NodeAddress, message_handler: MessageHandler) -> None:
        self._bind = bind
        self._message_handler = message_handler
        self._server: _ThreadedTCPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the TCP listener and background serving thread."""
        if self._thread and self._thread.is_alive():
            return
        self._server = _ThreadedTCPServer(
            (self._bind.host, self._bind.port),
            _ClusterRequestHandler,
            self._message_handler,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.2},
            name="cluster-tcp-server",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Shutdown server and wait for the serving thread to exit."""
        if not self._server:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread:
            self._thread.join(timeout=1.0)
        self._server = None
        self._thread = None


def request_response(
    peer: NodeAddress,
    message: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    """
    Send one TCP request to a peer and wait for one response.

    Parameters
    ----------
    peer:
        Target peer endpoint.
    message:
        JSON-serializable message envelope.
    timeout_seconds:
        Socket timeout applied to connect/send/receive steps.
    """
    with socket.create_connection((peer.host, peer.port), timeout=timeout_seconds) as sock:
        sock.settimeout(timeout_seconds)
        send_frame(sock, message)
        return recv_frame(sock)
