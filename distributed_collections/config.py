"""
Configuration models for distributed collection clusters.

This module centralizes all tunable runtime settings used by the cluster node:

* network binding/advertising
* static seed peer discovery
* multicast discovery
* protocol and retry timeouts

The configuration classes are intentionally explicit and heavily documented to
make deployment behavior easy to reason about in multi-node environments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


@dataclass(frozen=True, slots=True)
class NodeAddress:
    """
    TCP endpoint used for cluster communication.

    Parameters
    ----------
    host:
        DNS name or IP address that can be reached by cluster peers.
    port:
        TCP port where the cluster node listens.
    """

    host: str
    port: int

    def __post_init__(self) -> None:
        """Validate the host/port pair at construction time."""
        if not self.host:
            raise ValueError("NodeAddress.host must be a non-empty string.")
        if not (1 <= int(self.port) <= 65535):
            raise ValueError("NodeAddress.port must be in range 1..65535.")

    def as_dict(self) -> dict[str, object]:
        """
        Convert the address into a JSON-friendly dictionary.

        Returns
        -------
        dict[str, object]
            Mapping with ``host`` and ``port`` keys.
        """
        return {"host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "NodeAddress":
        """
        Create an address instance from a dictionary payload.

        Parameters
        ----------
        payload:
            Mapping that must contain ``host`` and ``port`` keys.
        """
        return cls(host=str(payload["host"]), port=int(payload["port"]))


class DiscoveryMode(str, Enum):
    """
    Supported cluster join/discovery strategies.

    STATIC
        Uses a predefined list of seed peers.
    MULTICAST
        Sends multicast discovery probes and collects responses.
    """

    STATIC = "static"
    MULTICAST = "multicast"


@dataclass(slots=True)
class StaticDiscoveryConfig:
    """
    Configuration for static peer discovery.

    Static discovery is deterministic and works well in environments where
    each node's address is known ahead of time (for example, static VMs).
    """

    seeds: list[NodeAddress] = field(default_factory=list)


@dataclass(slots=True)
class MulticastDiscoveryConfig:
    """
    Configuration for multicast-based peer discovery.

    Multicast discovery is convenient for local networks and dynamic
    environments where nodes can broadcast presence to discover each other.
    """

    group: str = "239.11.11.11"
    port: int = 55300
    timeout_seconds: float = 1.5
    discovery_attempts: int = 2
    ttl: int = 1


@dataclass(slots=True)
class ClusterConfig:
    """
    Top-level runtime configuration used by :class:`ClusterNode`.

    Parameters
    ----------
    cluster_name:
        Logical cluster namespace. Nodes only talk to peers with the same name.
    bind:
        Local address to bind the TCP listener to.
    advertise_host:
        Reachable host/IP that is shared with remote peers during discovery and
        handshakes. This can differ from ``bind.host`` when binding to
        ``0.0.0.0``.
    static_discovery:
        Static seed peer settings.
    multicast:
        Multicast discovery settings.
    enabled_discovery:
        Ordered join strategy list. The node runs each strategy and merges
        discovered peers.
    socket_timeout_seconds:
        Timeout for outbound TCP and UDP operations.
    reconnect_interval_seconds:
        Delay before re-running join logic when ``join_cluster`` is called
        repeatedly by user code.
    auto_sync_on_join:
        If true, the node requests a full data snapshot from a discovered peer
        right after join.
    """

    cluster_name: str = "default"
    bind: NodeAddress = field(default_factory=lambda: NodeAddress("0.0.0.0", 5701))
    advertise_host: str = "127.0.0.1"
    static_discovery: StaticDiscoveryConfig = field(default_factory=StaticDiscoveryConfig)
    multicast: MulticastDiscoveryConfig = field(default_factory=MulticastDiscoveryConfig)
    enabled_discovery: tuple[DiscoveryMode, ...] = (
        DiscoveryMode.STATIC,
        DiscoveryMode.MULTICAST,
    )
    socket_timeout_seconds: float = 2.0
    reconnect_interval_seconds: float = 3.0
    auto_sync_on_join: bool = True

    @property
    def advertise_address(self) -> NodeAddress:
        """
        Return the address peers should use for outbound TCP messages.

        Returns
        -------
        NodeAddress
            Address based on ``advertise_host`` and ``bind.port``.
        """
        return NodeAddress(self.advertise_host, self.bind.port)

    def normalized_static_seeds(self) -> list[NodeAddress]:
        """
        Return static seed peers after filtering out the local node.

        Returns
        -------
        list[NodeAddress]
            Deduplicated seed peers excluding self.
        """
        self_address = self.advertise_address
        unique = []
        seen = set()
        for seed in self.static_discovery.seeds:
            if seed == self_address:
                continue
            key = (seed.host, seed.port)
            if key in seen:
                continue
            seen.add(key)
            unique.append(seed)
        return unique
