"""
Peer discovery strategies for cluster formation.

Two discovery models are implemented:

* Static seed peers, where addresses are configured up front.
* Multicast discovery, where nodes announce presence over UDP multicast.

The cluster node can run both in one join cycle, merge discovered peers, and
attempt TCP handshakes with the union of addresses.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import socket
import struct
import threading
import time
from typing import Iterable

from .config import ClusterConfig, NodeAddress, _detect_current_node_address

_DISCOVER = "discover"
_DISCOVER_REPLY = "discover_reply"
_LOGGER = logging.getLogger(__name__)


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
            _LOGGER.debug(
                "Selected multicast interface address=%s from candidate=%s bind_host=%s advertise_host=%s",
                resolved,
                candidate,
                bind_host,
                advertise_host,
            )
            return resolved
    _LOGGER.debug(
        "Falling back multicast interface to wildcard bind_host=%s advertise_host=%s",
        bind_host,
        advertise_host,
    )
    return "0.0.0.0"


def discover_static_peers(config: ClusterConfig) -> list[NodeAddress]:
    """
    Return static seed peers from configuration.

    Parameters
    ----------
    config:
        Cluster configuration holding seed addresses.
    """
    peers = config.normalized_static_seeds()
    _LOGGER.debug(
        "Static discovery resolved peers=%s count=%d",
        [(peer.host, peer.port) for peer in peers],
        len(peers),
    )
    return peers


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
            _LOGGER.debug("Multicast responder already running node_id=%s", self._node_id)
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="multicast-discovery-responder",
            daemon=True,
        )
        self._thread.start()
        _LOGGER.debug(
            "Multicast responder started cluster=%s node_id=%s group=%s port=%d",
            self._cluster_name,
            self._node_id,
            self._group,
            self._port,
        )

    def stop(self) -> None:
        """Signal thread shutdown and wait briefly for completion."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        _LOGGER.debug("Multicast responder stopped node_id=%s", self._node_id)

    def _resolve_reply_address(self) -> NodeAddress:
        """
        Resolve advertised address used in discovery replies.

        When auto-advertise behavior is enabled, prefer the currently assigned
        node interface IP to mirror Hazelcast-style multicast identity.
        """
        if not self._prefer_current_assigned_ip:
            _LOGGER.debug(
                "Using configured advertise address for discovery reply host=%s port=%d",
                self._advertise.host,
                self._advertise.port,
            )
            return self._advertise
        detected = _current_multicast_interface_ip(self._bind_host, self._advertise.host)
        if detected == "0.0.0.0":
            detected = _detect_current_node_address(self._bind_host)
        try:
            resolved = NodeAddress(host=detected, port=self._advertise.port)
            _LOGGER.debug(
                "Resolved dynamic discovery reply address host=%s port=%d",
                resolved.host,
                resolved.port,
            )
            return resolved
        except ValueError:
            _LOGGER.debug(
                "Dynamic discovery reply address invalid, falling back to advertise host=%s port=%d",
                self._advertise.host,
                self._advertise.port,
            )
            return self._advertise

    def _open_receiver_socket(self, interface_ip: str) -> socket.socket | None:
        """
        Open and configure multicast receiver socket.

        Returns ``None`` when receiver cannot be started, allowing caller to
        degrade gracefully instead of failing node startup.
        """
        _LOGGER.debug(
            "Opening multicast receiver socket group=%s port=%d interface_ip=%s",
            self._group,
            self._port,
            interface_ip,
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                if hasattr(socket, "SO_REUSEPORT"):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

            try:
                sock.bind(("", self._port))
            except OSError as exc:
                if exc.errno in {errno.EADDRINUSE, 48, 98}:
                    _LOGGER.warning(
                        "Multicast responder port already in use; skipping responder for this node. "
                        "port=%s reason=%s",
                        self._port,
                        exc,
                    )
                    return None
                raise

            membership_interfaces = [interface_ip]
            if interface_ip != "0.0.0.0":
                membership_interfaces.append("0.0.0.0")
            joined_group = False
            for membership_interface in membership_interfaces:
                try:
                    membership = struct.pack(
                        "4s4s",
                        socket.inet_aton(self._group),
                        socket.inet_aton(membership_interface),
                    )
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
                    joined_group = True
                    _LOGGER.debug(
                        "Joined multicast group=%s port=%d interface=%s",
                        self._group,
                        self._port,
                        membership_interface,
                    )
                except OSError:
                    continue

            if not joined_group:
                _LOGGER.warning(
                    "Failed to join multicast group; responder disabled. "
                    "group=%s port=%s interface_ip=%s",
                    self._group,
                    self._port,
                    interface_ip,
                )
                return None

            sock.settimeout(self._socket_timeout_seconds)
            _LOGGER.debug(
                "Multicast receiver ready group=%s port=%d timeout=%s",
                self._group,
                self._port,
                self._socket_timeout_seconds,
            )
            return sock
        except Exception:
            try:
                sock.close()
            except OSError:
                pass
            raise

    def _run(self) -> None:
        interface_ip = _current_multicast_interface_ip(self._bind_host, self._advertise.host)
        _LOGGER.debug(
            "Multicast responder loop starting cluster=%s node_id=%s interface_ip=%s",
            self._cluster_name,
            self._node_id,
            interface_ip,
        )
        recv_sock = self._open_receiver_socket(interface_ip)
        if recv_sock is None:
            _LOGGER.debug("Multicast responder disabled due to socket setup failure node_id=%s", self._node_id)
            return
        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            while not self._stop.is_set():
                try:
                    payload, sender = recv_sock.recvfrom(65535)
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
                _LOGGER.debug(
                    "Received multicast discovery probe from=%s cluster=%s remote_node_id=%s",
                    sender,
                    message.get("cluster"),
                    message.get("node_id"),
                )

                reply_address = self._resolve_reply_address()
                reply = {
                    "kind": _DISCOVER_REPLY,
                    "cluster": self._cluster_name,
                    "node_id": self._node_id,
                    "address": reply_address.as_dict(),
                }
                raw = json.dumps(reply, separators=(",", ":"), sort_keys=True).encode("utf-8")
                try:
                    send_sock.sendto(raw, sender)
                    _LOGGER.debug(
                        "Sent multicast discovery reply to=%s host=%s port=%d",
                        sender,
                        reply_address.host,
                        reply_address.port,
                    )
                except OSError:
                    continue
        finally:
            _LOGGER.debug("Multicast responder loop stopping node_id=%s", self._node_id)
            try:
                recv_sock.close()
            except OSError:
                pass
            try:
                send_sock.close()
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

    attempts = max(1, config.multicast.discovery_attempts)
    _LOGGER.debug(
        "Starting multicast discovery cluster=%s node_id=%s group=%s port=%d attempts=%d",
        config.cluster_name,
        node_id,
        config.multicast.group,
        config.multicast.port,
        attempts,
    )
    for attempt in range(attempts):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            interface_ip = _current_multicast_interface_ip(config.bind.host, probe_address.host)
            ttl = int(config.multicast.ttl)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", ttl))
            # Keep multicast loopback enabled so multiple local instances can
            # discover each other on the same host.
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            sock.settimeout(socket_timeout_seconds)

            send_interfaces = [interface_ip]
            if interface_ip != "0.0.0.0":
                send_interfaces.extend(["0.0.0.0", "127.0.0.1"])
            deduped_send_interfaces: list[str] = []
            for candidate in send_interfaces:
                if candidate not in deduped_send_interfaces:
                    deduped_send_interfaces.append(candidate)
            _LOGGER.debug(
                "Multicast discovery attempt=%d/%d interface_ip=%s send_interfaces=%s",
                attempt + 1,
                attempts,
                interface_ip,
                deduped_send_interfaces,
            )

            sent = False
            last_error: OSError | None = None
            for send_interface in deduped_send_interfaces:
                try:
                    if send_interface != "0.0.0.0":
                        sock.setsockopt(
                            socket.IPPROTO_IP,
                            socket.IP_MULTICAST_IF,
                            socket.inet_aton(send_interface),
                        )
                    sock.sendto(wire, (config.multicast.group, config.multicast.port))
                    sent = True
                    _LOGGER.debug(
                        "Multicast probe sent via interface=%s group=%s port=%d",
                        send_interface,
                        config.multicast.group,
                        config.multicast.port,
                    )
                except OSError as exc:
                    last_error = exc
                    continue
            if not sent:
                _LOGGER.warning(
                    "Multicast probe send failed; continuing without multicast peers. "
                    "group=%s port=%s interface_ip=%s reason=%s",
                    config.multicast.group,
                    config.multicast.port,
                    interface_ip,
                    last_error,
                )
                continue

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
                _LOGGER.debug(
                    "Discovered multicast peer host=%s port=%d node_id=%s",
                    address.host,
                    address.port,
                    message.get("node_id"),
                )
        finally:
            try:
                sock.close()
            except OSError:
                pass

    peers = list(discovered.values())
    _LOGGER.debug(
        "Multicast discovery completed node_id=%s discovered_count=%d peers=%s",
        node_id,
        len(peers),
        [(peer.host, peer.port) for peer in peers],
    )
    return peers


def merge_discovery_results(groups: Iterable[list[NodeAddress]]) -> list[NodeAddress]:
    """
    Merge multiple peer lists into one deduplicated address list.

    The function keeps the first occurrence order so discovery strategy order
    remains meaningful and predictable.
    """
    merged: list[NodeAddress] = []
    seen: set[tuple[str, int]] = set()
    group_count = 0
    for peers in groups:
        group_count += 1
        for peer in peers:
            key = (peer.host, peer.port)
            if key in seen:
                continue
            seen.add(key)
            merged.append(peer)
    _LOGGER.debug(
        "Merged discovery results groups=%d merged_count=%d peers=%s",
        group_count,
        len(merged),
        [(peer.host, peer.port) for peer in merged],
    )
    return merged
