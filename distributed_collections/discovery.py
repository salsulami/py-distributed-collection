"""
Peer discovery strategies for cluster formation.

Two discovery models are implemented:

* Static seed peers, where addresses are configured up front.
* Multicast discovery, where nodes announce presence over UDP multicast.

The cluster node can run both in one join cycle, merge discovered peers, and
attempt TCP handshakes with the union of addresses.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import threading
import time
from typing import Iterable

from .config import ClusterConfig, NodeAddress, _detect_current_node_address

_DISCOVER = "discover"
_DISCOVER_REPLY = "discover_reply"


def _resolve_ipv4_host(host: str) -> str | None:
    candidate = str(host).strip()
    if not candidate:
        return None
    try:
        socket.inet_aton(candidate)
        return candidate
    except OSError:
        pass
    try:
        resolved = socket.gethostbyname(candidate).strip()
    except OSError:
        return None
    if not resolved:
        return None
    try:
        socket.inet_aton(resolved)
    except OSError:
        return None
    return resolved


def _current_multicast_interface_ip(bind_host: str, advertise_host: str) -> str:
    """
    Resolve the current assigned IPv4 address used for multicast traffic.
    """
    interface_override = os.getenv("DISTRIBUTED_COLLECTIONS_MULTICAST_INTERFACE_IP", "").strip()
    candidates = (
        interface_override,
        _detect_current_node_address(bind_host),
        bind_host,
        advertise_host,
    )
    for candidate in candidates:
        resolved = _resolve_ipv4_host(candidate)
        if resolved and resolved != "0.0.0.0":
            return resolved
    return "0.0.0.0"


def discover_static_peers(config: ClusterConfig) -> list[NodeAddress]:
    """
    Return static seed peers from configuration.

    Parameters
    ----------
    config:
        Cluster configuration holding seed addresses.
    """
    return config.normalized_static_seeds()


class MulticastDiscoveryResponder:
    """
    UDP multicast listener that replies to discovery probes.

    The responder runs in a background daemon thread after node startup and
    returns the node's advertised TCP endpoint to requesting peers.
    """

    def __init__(
        self,
        *,
        cluster_name: str,
        node_id: str,
        advertise: NodeAddress,
        bind_host: str,
        group: str,
        port: int,
        socket_timeout_seconds: float,
        prefer_current_assigned_ip: bool = False,
    ) -> None:
        self._cluster_name = cluster_name
        self._node_id = node_id
        self._advertise = advertise
        self._bind_host = bind_host
        self._group = group
        self._port = port
        self._socket_timeout_seconds = socket_timeout_seconds
        self._prefer_current_assigned_ip = prefer_current_assigned_ip
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Start the background listener thread if not already running."""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="multicast-discovery-responder",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal thread shutdown and wait briefly for completion."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            interface_ip = _current_multicast_interface_ip(self._bind_host, self._advertise.host)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", self._port))
            try:
                if interface_ip != "0.0.0.0":
                    sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(interface_ip),
                    )
            except OSError:
                pass
            membership_interface = interface_ip if interface_ip != "0.0.0.0" else "0.0.0.0"
            membership = struct.pack(
                "4s4s",
                socket.inet_aton(self._group),
                socket.inet_aton(membership_interface),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
            sock.settimeout(self._socket_timeout_seconds)

            while not self._stop.is_set():
                try:
                    payload, sender = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break

                try:
                    message = json.loads(payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if message.get("kind") != _DISCOVER:
                    continue
                if message.get("cluster") != self._cluster_name:
                    continue
                if message.get("node_id") == self._node_id:
                    continue

                reply_address = self._advertise
                if self._prefer_current_assigned_ip:
                    detected = _current_multicast_interface_ip(self._bind_host, self._advertise.host)
                    if detected == "0.0.0.0":
                        detected = _detect_current_node_address(self._bind_host)
                    try:
                        reply_address = NodeAddress(host=detected, port=self._advertise.port)
                    except ValueError:
                        reply_address = self._advertise

                reply = {
                    "kind": _DISCOVER_REPLY,
                    "cluster": self._cluster_name,
                    "node_id": self._node_id,
                    "address": reply_address.as_dict(),
                }
                raw = json.dumps(reply, separators=(",", ":"), sort_keys=True).encode("utf-8")
                try:
                    sock.sendto(raw, sender)
                except OSError:
                    continue
        finally:
            try:
                sock.close()
            except OSError:
                pass


def discover_multicast_peers(
    *,
    config: ClusterConfig,
    node_id: str,
    socket_timeout_seconds: float,
) -> list[NodeAddress]:
    """
    Discover peers by broadcasting multicast probes and collecting replies.

    Parameters
    ----------
    config:
        Cluster configuration containing multicast settings.
    node_id:
        Unique ID of the local node. Replies from self are ignored.
    socket_timeout_seconds:
        Per-attempt receive timeout.

    Returns
    -------
    list[NodeAddress]
        Unique peer addresses discovered from multicast listeners.
    """
    discovered: dict[tuple[str, int], NodeAddress] = {}
    probe_address = config.advertise_address
    if bool(getattr(config, "_auto_advertise_host", False)):
        detected = _current_multicast_interface_ip(config.bind.host, config.advertise_address.host)
        if detected == "0.0.0.0":
            detected = _detect_current_node_address(config.bind.host)
        try:
            probe_address = NodeAddress(host=detected, port=config.bind.port)
        except ValueError:
            probe_address = config.advertise_address
    probe = {
        "kind": _DISCOVER,
        "cluster": config.cluster_name,
        "node_id": node_id,
        "address": probe_address.as_dict(),
    }
    wire = json.dumps(probe, separators=(",", ":"), sort_keys=True).encode("utf-8")

    for _ in range(max(1, config.multicast.discovery_attempts)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            interface_ip = _current_multicast_interface_ip(config.bind.host, probe_address.host)
            ttl = int(config.multicast.ttl)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", ttl))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
            try:
                if interface_ip != "0.0.0.0":
                    sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(interface_ip),
                    )
            except OSError:
                pass
            sock.settimeout(socket_timeout_seconds)

            sock.sendto(wire, (config.multicast.group, config.multicast.port))

            deadline = time.monotonic() + float(config.multicast.timeout_seconds)
            while time.monotonic() < deadline:
                try:
                    payload, _sender = sock.recvfrom(65535)
                except socket.timeout:
                    break
                except OSError:
                    break

                try:
                    message = json.loads(payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                if message.get("kind") != _DISCOVER_REPLY:
                    continue
                if message.get("cluster") != config.cluster_name:
                    continue
                if message.get("node_id") == node_id:
                    continue

                address_raw = message.get("address")
                if not isinstance(address_raw, dict):
                    continue
                try:
                    address = NodeAddress.from_dict(address_raw)
                except (KeyError, TypeError, ValueError):
                    continue
                if address == config.advertise_address:
                    continue
                discovered[(address.host, address.port)] = address
        finally:
            try:
                sock.close()
            except OSError:
                pass

    return list(discovered.values())


def merge_discovery_results(groups: Iterable[list[NodeAddress]]) -> list[NodeAddress]:
    """
    Merge multiple peer lists into one deduplicated address list.

    The function keeps the first occurrence order so discovery strategy order
    remains meaningful and predictable.
    """
    merged: list[NodeAddress] = []
    seen: set[tuple[str, int]] = set()
    for peers in groups:
        for peer in peers:
            key = (peer.host, peer.port)
            if key in seen:
                continue
            seen.add(key)
            merged.append(peer)
    return merged
