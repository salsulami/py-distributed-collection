"""
Low-level message protocol utilities for cluster communication.

The protocol intentionally stays simple:

1. Every TCP frame starts with a 4-byte unsigned big-endian payload length.
2. The payload is UTF-8 JSON encoded.
3. Messages use the common envelope shape:

   ``{"kind": "...", "cluster": "...", "payload": {...}}``

This design keeps interoperability straightforward and makes on-wire traffic
easy to inspect during debugging.
"""

from __future__ import annotations

import json
import socket
import struct
from enum import Enum
from typing import Any

from .exceptions import ProtocolDecodeError

_HEADER = struct.Struct("!I")
_MAX_FRAME_BYTES = 8 * 1024 * 1024


class MessageKind(str, Enum):
    """
    Cluster message categories exchanged between peers.

    HANDSHAKE
        First message sent by a joining peer to exchange endpoint metadata.
    HANDSHAKE_ACK
        Response to ``HANDSHAKE`` including currently known members.
    STATE_REQUEST
        Snapshot request used to bootstrap state after join.
    STATE_RESPONSE
        Snapshot response carrying full map/list/queue state.
    OPERATION
        Replicated collection mutation (map/list/queue/topic).
    ERROR
        Error response for malformed or invalid requests.
    """

    HANDSHAKE = "handshake"
    HANDSHAKE_ACK = "handshake_ack"
    STATE_REQUEST = "state_request"
    STATE_RESPONSE = "state_response"
    OPERATION = "operation"
    ERROR = "error"


def make_message(kind: MessageKind, cluster: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Build a protocol message envelope.

    Parameters
    ----------
    kind:
        Message category enum value.
    cluster:
        Cluster namespace. Receivers use it as an isolation boundary.
    payload:
        Message-specific body.
    """
    return {"kind": kind.value, "cluster": cluster, "payload": payload}


def encode_frame(message: dict[str, Any]) -> bytes:
    """
    Encode a message into a length-prefixed binary frame.

    Returns
    -------
    bytes
        Byte sequence ready to be sent over a TCP socket.
    """
    body = json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(body) > _MAX_FRAME_BYTES:
        raise ProtocolDecodeError(
            f"Message size {len(body)} exceeds max frame {_MAX_FRAME_BYTES} bytes."
        )
    return _HEADER.pack(len(body)) + body


def decode_message(body: bytes) -> dict[str, Any]:
    """
    Decode a raw JSON payload into a dictionary message.

    Raises
    ------
    ProtocolDecodeError
        If payload is not valid UTF-8 JSON or not a dictionary.
    """
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolDecodeError("Failed to decode JSON payload.") from exc
    if not isinstance(parsed, dict):
        raise ProtocolDecodeError("Decoded message must be a JSON object.")
    return parsed


def _read_exact(sock: socket.socket, total: int) -> bytes:
    """Read exactly ``total`` bytes or raise if the stream closes early."""
    chunks = bytearray()
    while len(chunks) < total:
        block = sock.recv(total - len(chunks))
        if not block:
            raise ProtocolDecodeError("Peer closed connection before full frame arrived.")
        chunks.extend(block)
    return bytes(chunks)


def recv_frame(sock: socket.socket) -> dict[str, Any]:
    """
    Read and decode a single frame from a TCP socket.

    Parameters
    ----------
    sock:
        Connected socket object.
    """
    header = _read_exact(sock, _HEADER.size)
    (length,) = _HEADER.unpack(header)
    if length > _MAX_FRAME_BYTES:
        raise ProtocolDecodeError(
            f"Incoming frame {length} exceeds max frame {_MAX_FRAME_BYTES}."
        )
    body = _read_exact(sock, length)
    return decode_message(body)


def send_frame(sock: socket.socket, message: dict[str, Any]) -> None:
    """
    Encode and send a complete message frame over the socket.

    The function delegates framing details to :func:`encode_frame` and uses
    ``sendall`` to guarantee full transmission before returning.
    """
    sock.sendall(encode_frame(message))
