"""
Redis-backed cluster node convenience wrapper.
"""

from __future__ import annotations

from distributed_collections.cluster import ClusterNode
from distributed_collections.config import ClusterConfig
from redis import Redis

from .store import RedisClusterDataStore, RedisStoreConfig


class RedisClusterNode(ClusterNode):
    """
    :class:`ClusterNode` variant that persists collection state in Redis.

    Parameters
    ----------
    config:
        Standard core cluster configuration.
    redis_url:
        Redis URL used when ``redis_client`` is not supplied.
    namespace:
        Key prefix namespace for all collection data.
    redis_client:
        Optional preconfigured Redis client instance.
    """

    def __init__(
        self,
        config: ClusterConfig,
        *,
        redis_url: str = "redis://127.0.0.1:6379/0",
        namespace: str = "distributed-collections",
        redis_client: Redis | None = None,
    ) -> None:
        self.redis_store = RedisClusterDataStore(
            config=RedisStoreConfig(redis_url=redis_url, namespace=namespace),
            redis_client=redis_client,
        )
        super().__init__(config, store=self.redis_store)
