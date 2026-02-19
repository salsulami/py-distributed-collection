"""
Redis backend plugin for distributed_python_collections.

This package is intentionally separate from the core library so users can opt
into Redis-backed state storage only when needed:

    from distributed_collections import ClusterConfig
    from distributed_collections_redis import RedisClusterNode

    node = RedisClusterNode(
        ClusterConfig(cluster_name="orders"),
        redis_url="redis://127.0.0.1:6379/0",
        namespace="orders-cluster",
    )

Redis mode uses Redis as a shared source of truth for map/list/queue data, so
cluster nodes do not need peer-to-peer state replication for those collections.

Users can either import this package directly or use the core backend factory:

    from distributed_collections import create_node
    node = create_node(config, backend="redis", redis_url="redis://127.0.0.1:6379/0")
"""

from .node import RedisClusterNode
from .store import RedisClusterDataStore, RedisStoreConfig

__all__ = ["RedisClusterDataStore", "RedisClusterNode", "RedisStoreConfig"]
