"""
Configuration models for distributed collection clusters.

This module centralizes all tunable runtime settings used by the cluster node:

* network binding/advertising
* static seed peer discovery
* multicast discovery
* protocol and retry timeouts
* transport security and authentication
* async replication retry behavior
* snapshot persistence

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
class SecurityConfig:
    """
    Security settings for cluster transport authentication.

    The current runtime supports shared-token HMAC signatures over all TCP
    protocol messages. When ``shared_token`` is set, every outbound message is
    signed and every inbound message must pass signature validation.
    """

    shared_token: str | None = None


@dataclass(slots=True)
class ReplicationConfig:
    """
    Replication delivery settings for asynchronous operation fan-out.

    Notes
    -----
    Replication workers run in background daemon threads and retry failed peer
    deliveries with exponential backoff.
    """

    worker_threads: int = 2
    max_retries: int = 4
    initial_backoff_seconds: float = 0.05
    max_backoff_seconds: float = 1.0
    member_failure_threshold: int = 5


@dataclass(slots=True)
class PersistenceConfig:
    """
    Snapshot persistence settings for node restart recovery.

    When enabled, nodes periodically flush full in-memory snapshots to the
    configured JSON file path and reload it on startup.
    """

    enabled: bool = False
    snapshot_path: str = "cluster_snapshot.json"
    fsync: bool = True


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
    security:
        Transport authentication settings. Use a non-empty shared token in
        production deployments.
    replication:
        Asynchronous replication queue and retry behavior.
    persistence:
        Local snapshot persistence and restart recovery behavior.
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
    security: SecurityConfig = field(default_factory=SecurityConfig)
    replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)

    def __post_init__(self) -> None:
        """Validate configuration values that affect runtime safety."""
        if not self.cluster_name:
            raise ValueError("ClusterConfig.cluster_name must be non-empty.")
        if self.socket_timeout_seconds <= 0:
            raise ValueError("ClusterConfig.socket_timeout_seconds must be > 0.")
        if self.reconnect_interval_seconds < 0:
            raise ValueError("ClusterConfig.reconnect_interval_seconds must be >= 0.")

        if self.replication.worker_threads <= 0:
            raise ValueError("ReplicationConfig.worker_threads must be >= 1.")
        if self.replication.max_retries <= 0:
            raise ValueError("ReplicationConfig.max_retries must be >= 1.")
        if self.replication.initial_backoff_seconds < 0:
            raise ValueError("ReplicationConfig.initial_backoff_seconds must be >= 0.")
        if self.replication.max_backoff_seconds < self.replication.initial_backoff_seconds:
            raise ValueError(
                "ReplicationConfig.max_backoff_seconds must be >= initial_backoff_seconds."
            )
        if self.replication.member_failure_threshold <= 0:
            raise ValueError("ReplicationConfig.member_failure_threshold must be >= 1.")

        token = self.security.shared_token
        if token is not None and not token.strip():
            raise ValueError("SecurityConfig.shared_token cannot be blank when provided.")

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
