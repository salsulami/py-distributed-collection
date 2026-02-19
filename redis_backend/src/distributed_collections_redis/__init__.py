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
"""

from .node import RedisClusterNode
from .store import RedisClusterDataStore, RedisStoreConfig

__all__ = ["RedisClusterDataStore", "RedisClusterNode", "RedisStoreConfig"]
