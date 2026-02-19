"""
TCP transport layer for peer-to-peer cluster messaging.

The transport module provides two focused primitives:

* :class:`TcpTransportServer` for inbound message handling.
* :func:`request_response` for outbound request/response exchange.

Protocol framing and JSON encoding are delegated to :mod:`protocol`.
Outbound request/response calls also enforce protocol-version compatibility and
optional shared-token authentication.
When configured, both server and client sockets are wrapped with TLS.
"""

from __future__ import annotations

import socket
import socketserver
import ssl
import threading
from typing import Any, Callable

from .config import NodeAddress
from .protocol import (
    PROTOCOL_VERSION,
    assert_authenticated,
    assert_protocol_compatible,
    attach_authentication,
    recv_frame,
    send_frame,
)

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
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        super().__init__(server_address, handler_cls)
        self.message_handler = message_handler
        self.ssl_context = ssl_context


class _ClusterRequestHandler(socketserver.BaseRequestHandler):
    """Handle one inbound framed message and send one response."""

    def handle(self) -> None:
        raw_sock = self.request
        if not isinstance(raw_sock, socket.socket):
            return
        response: dict[str, Any]
        active_sock: socket.socket | ssl.SSLSocket = raw_sock
        ssl_context = getattr(self.server, "ssl_context", None)  # type: ignore[attr-defined]
        try:
            if ssl_context is not None:
                active_sock = ssl_context.wrap_socket(raw_sock, server_side=True)
            incoming = recv_frame(active_sock)
            response = self.server.message_handler(incoming)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - transport must not crash the server loop
            response = {
                "kind": "error",
                "cluster": "",
                "protocol_version": PROTOCOL_VERSION,
                "payload": {"reason": str(exc)},
            }
        send_frame(active_sock, response)


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

    def __init__(
        self,
        *,
        bind: NodeAddress,
        message_handler: MessageHandler,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._bind = bind
        self._message_handler = message_handler
        self._ssl_context = ssl_context
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
            self._ssl_context,
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
    *,
    security_token: str | None = None,
    ssl_context: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
    min_protocol_version: int = PROTOCOL_VERSION,
    max_protocol_version: int = PROTOCOL_VERSION,
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
    security_token:
        Optional shared token for HMAC message authentication.
    ssl_context:
        Optional client SSL context used to wrap TCP connections.
    server_hostname:
        Optional SNI/hostname value for TLS certificate validation.
    min_protocol_version:
        Minimum accepted peer protocol version.
    max_protocol_version:
        Maximum accepted peer protocol version.
    """
    with socket.create_connection((peer.host, peer.port), timeout=timeout_seconds) as raw_sock:
        raw_sock.settimeout(timeout_seconds)
        active_sock: socket.socket | ssl.SSLSocket = raw_sock
        if ssl_context is not None:
            active_sock = ssl_context.wrap_socket(
                raw_sock,
                server_hostname=server_hostname or peer.host,
            )
            active_sock.settimeout(timeout_seconds)
        send_frame(active_sock, attach_authentication(message, security_token))
        response = recv_frame(active_sock)
        assert_protocol_compatible(
            response,
            min_supported_version=min_protocol_version,
            max_supported_version=max_protocol_version,
        )
        assert_authenticated(response, security_token)
        return response
