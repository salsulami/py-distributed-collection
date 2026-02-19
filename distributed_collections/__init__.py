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

Redis-backed implementation is available as a separate plugin package:

* project: ``distributed-python-collections-redis``
* import path: ``distributed_collections_redis``
* in Redis mode, map/list/queue state is centralized in Redis and not
  synchronized through node-to-node collection replication

Store switching can be done with one parameter:

    from distributed_collections import create_node

    node = create_node(config, backend="memory")
    node = create_node(config, backend="redis", redis_url="redis://127.0.0.1:6379/0")

Or with classmethod syntax:

    node = ClusterNode.from_backend(config, backend="memory")
    node = ClusterNode.from_backend(config, backend="redis", redis_url="redis://127.0.0.1:6379/0")

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
from .backends import StoreBackend, available_backends, create_node, create_store
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
from .store import ClusterDataStore
from .store_protocol import CollectionStore

__all__ = [
    "ClusterConfig",
    "ClusterNode",
    "StoreBackend",
    "available_backends",
    "create_node",
    "create_store",
    "ACLConfig",
    "ConsensusConfig",
    "ConsistencyConfig",
    "ConsistencyMode",
    "DiscoveryMode",
    "DistributedList",
    "DistributedMap",
    "DistributedQueue",
    "DistributedTopic",
    "ClusterDataStore",
    "CollectionStore",
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
