"""
distributed_collections
=======================

Hazelcast-inspired distributed collections for Python applications.

The package provides a small, pragmatic API for building multi-node Python
applications where collection state is replicated over TCP:

* :class:`distributed_collections.primitives.DistributedMap`
* :class:`distributed_collections.primitives.DistributedList`
* :class:`distributed_collections.primitives.DistributedQueue`
* :class:`distributed_collections.primitives.DistributedTopic`

Discovery supports both static seed peers and multicast peer discovery.
Nodes exchange membership metadata and operation replication messages using a
compact length-prefixed JSON protocol on TCP.

Production hardening features include:

* optional shared-token HMAC authentication for all TCP messages
* optional TLS/mTLS transport encryption
* sender-based ACL enforcement for remote operations
* leader election + heartbeat failure detection + split-brain protection lease
* quorum/all/linearizable write consistency options
* asynchronous replication workers with retry/backoff and member eviction
* write-ahead log replay and checkpointed snapshot durability lifecycle
* optional on-disk snapshot persistence for restart recovery
* runtime counters via :meth:`distributed_collections.cluster.ClusterNode.stats`
* optional HTTP health/metrics/traces observability endpoint
* rolling-upgrade protocol compatibility ranges

Typical usage::

    from distributed_collections import ClusterConfig, ClusterNode, NodeAddress

    node = ClusterNode(
        ClusterConfig(
            cluster_name="orders",
            bind=NodeAddress("0.0.0.0", 5701),
            advertise_host="10.0.0.5",
        )
    )
    node.start()

    users = node.get_map("users")
    users.put("u-1", {"name": "Alice"})

    node.stop()
"""

from .cluster import ClusterNode
from .config import (
    ACLConfig,
    ClusterConfig,
    ConsensusConfig,
    ConsistencyConfig,
    ConsistencyMode,
    DiscoveryMode,
    MulticastDiscoveryConfig,
    NodeAddress,
    ObservabilityConfig,
    PersistenceConfig,
    ReplicationConfig,
    SecurityConfig,
    StaticDiscoveryConfig,
    TLSConfig,
    UpgradeConfig,
    WriteAheadLogConfig,
)
from .primitives import (
    DistributedList,
    DistributedMap,
    DistributedQueue,
    DistributedTopic,
)

__all__ = [
    "ClusterConfig",
    "ClusterNode",
    "ACLConfig",
    "ConsensusConfig",
    "ConsistencyConfig",
    "ConsistencyMode",
    "DiscoveryMode",
    "DistributedList",
    "DistributedMap",
    "DistributedQueue",
    "DistributedTopic",
    "MulticastDiscoveryConfig",
    "NodeAddress",
    "ObservabilityConfig",
    "PersistenceConfig",
    "ReplicationConfig",
    "SecurityConfig",
    "StaticDiscoveryConfig",
    "TLSConfig",
    "UpgradeConfig",
    "WriteAheadLogConfig",
]
