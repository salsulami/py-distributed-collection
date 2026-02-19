"""
Redis-backed collection store implementation.

The store implements the core ``CollectionStore`` protocol and can be injected
into :class:`distributed_collections.cluster.ClusterNode`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Any

from distributed_collections.exceptions import UnsupportedOperationError
from redis import Redis

_POP_INDEX_LUA = """
local key = KEYS[1]
local idx = tonumber(ARGV[1])
local marker = ARGV[2]
local len = redis.call('LLEN', key)
if len == 0 then
  return false
end
if idx < 0 then
  idx = len + idx
end
if idx < 0 or idx >= len then
  return false
end
local val = redis.call('LINDEX', key, idx)
redis.call('LSET', key, idx, marker)
redis.call('LREM', key, 1, marker)
return val
"""


@dataclass(slots=True)
class RedisStoreConfig:
    """
    Configuration for :class:`RedisClusterDataStore`.

    Parameters
    ----------
    redis_url:
        Redis connection URL used when a client is not directly supplied.
    namespace:
        Prefix for all redis keys created by this store.
    """

    redis_url: str = "redis://127.0.0.1:6379/0"
    namespace: str = "distributed-collections"


class RedisClusterDataStore:
    """
    Redis-backed implementation of the core distributed collection store API.

    Data model
    ----------
    * map values are stored in Redis hashes
    * list values are stored in Redis lists
    * queue values are stored in Redis lists (head = left, tail = right)
    """

    def __init__(
        self,
        *,
        config: RedisStoreConfig | None = None,
        redis_client: Redis | None = None,
    ) -> None:
        self.config = config or RedisStoreConfig()
        self._redis = redis_client or Redis.from_url(self.config.redis_url)
        self._lock = RLock()
        self._pop_index_script = self._redis.register_script(_POP_INDEX_LUA)
        self._marker = "__distributed_collections_pop_marker__"

    # ------------------------------------------------------------------ #
    # Key helpers
    # ------------------------------------------------------------------ #

    def _key_map(self, name: str) -> str:
        return f"{self.config.namespace}:map:{name}"

    def _key_list(self, name: str) -> str:
        return f"{self.config.namespace}:list:{name}"

    def _key_queue(self, name: str) -> str:
        return f"{self.config.namespace}:queue:{name}"

    def _idx_maps(self) -> str:
        return f"{self.config.namespace}:idx:maps"

    def _idx_lists(self) -> str:
        return f"{self.config.namespace}:idx:lists"

    def _idx_queues(self) -> str:
        return f"{self.config.namespace}:idx:queues"

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def _encode(self, value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    def _decode(self, value: bytes | str | None) -> Any:
        if value is None:
            return None
        if isinstance(value, bytes):
            raw = value.decode("utf-8")
        else:
            raw = value
        return json.loads(raw)

    def _decode_text(self, value: bytes | str) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    # ------------------------------------------------------------------ #
    # Mutation API
    # ------------------------------------------------------------------ #

    def apply_mutation(self, mutation: dict[str, Any]) -> Any:
        """
        Apply one collection mutation to Redis.
        """
        collection = str(mutation["collection"])
        name = str(mutation["name"])
        action = str(mutation["action"])

        with self._lock:
            if collection == "map":
                return self._apply_map_mutation(name=name, action=action, mutation=mutation)
            if collection == "list":
                return self._apply_list_mutation(name=name, action=action, mutation=mutation)
            if collection == "queue":
                return self._apply_queue_mutation(name=name, action=action, mutation=mutation)
            raise UnsupportedOperationError(f"Unknown collection type: {collection!r}")

    def _apply_map_mutation(self, *, name: str, action: str, mutation: dict[str, Any]) -> Any:
        key = self._key_map(name)
        self._redis.sadd(self._idx_maps(), name)
        if action == "put":
            field = str(mutation["key"])
            previous = self._decode(self._redis.hget(key, field))
            self._redis.hset(key, field, self._encode(mutation["value"]))
            return previous
        if action == "remove":
            field = str(mutation["key"])
            previous = self._decode(self._redis.hget(key, field))
            self._redis.hdel(key, field)
            if self._redis.hlen(key) == 0:
                self._redis.srem(self._idx_maps(), name)
            return previous
        if action == "clear":
            self._redis.delete(key)
            self._redis.srem(self._idx_maps(), name)
            return None
        raise UnsupportedOperationError(f"Unknown map action: {action!r}")

    def _apply_list_mutation(self, *, name: str, action: str, mutation: dict[str, Any]) -> Any:
        key = self._key_list(name)
        if action == "append":
            self._redis.rpush(key, self._encode(mutation["value"]))
            self._redis.sadd(self._idx_lists(), name)
            return None
        if action == "extend":
            values = [self._encode(item) for item in list(mutation["values"])]
            if values:
                self._redis.rpush(key, *values)
                self._redis.sadd(self._idx_lists(), name)
            return None
        if action == "set":
            index = int(mutation["index"])
            previous = self._decode(self._redis.lindex(key, index))
            self._redis.lset(key, index, self._encode(mutation["value"]))
            return previous
        if action == "pop":
            index = mutation.get("index")
            if index is None:
                value = self._redis.rpop(key)
                if self._redis.llen(key) == 0:
                    self._redis.srem(self._idx_lists(), name)
                return self._decode(value)
            popped = self._pop_index_script(
                keys=[key],
                args=[int(index), self._marker],
            )
            if self._redis.llen(key) == 0:
                self._redis.srem(self._idx_lists(), name)
            return self._decode(popped)
        if action == "remove_value":
            value = self._encode(mutation["value"])
            removed = int(self._redis.lrem(key, 1, value))
            if self._redis.llen(key) == 0:
                self._redis.srem(self._idx_lists(), name)
            return removed > 0
        if action == "clear":
            self._redis.delete(key)
            self._redis.srem(self._idx_lists(), name)
            return None
        raise UnsupportedOperationError(f"Unknown list action: {action!r}")

    def _apply_queue_mutation(self, *, name: str, action: str, mutation: dict[str, Any]) -> Any:
        key = self._key_queue(name)
        if action == "offer":
            self._redis.rpush(key, self._encode(mutation["value"]))
            self._redis.sadd(self._idx_queues(), name)
            return None
        if action == "poll":
            value = self._redis.lpop(key)
            if self._redis.llen(key) == 0:
                self._redis.srem(self._idx_queues(), name)
            return self._decode(value)
        if action == "clear":
            self._redis.delete(key)
            self._redis.srem(self._idx_queues(), name)
            return None
        raise UnsupportedOperationError(f"Unknown queue action: {action!r}")

    # ------------------------------------------------------------------ #
    # Read API
    # ------------------------------------------------------------------ #

    def map_get(self, name: str, key: str, default: Any = None) -> Any:
        value = self._redis.hget(self._key_map(name), key)
        if value is None:
            return default
        return self._decode(value)

    def map_items(self, name: str) -> dict[str, Any]:
        raw = self._redis.hgetall(self._key_map(name))
        return {
            self._decode_text(field): self._decode(value)
            for field, value in raw.items()
        }

    def map_contains(self, name: str, key: str) -> bool:
        return bool(self._redis.hexists(self._key_map(name), key))

    def map_size(self, name: str) -> int:
        return int(self._redis.hlen(self._key_map(name)))

    def list_values(self, name: str) -> list[Any]:
        raw = self._redis.lrange(self._key_list(name), 0, -1)
        return [self._decode(item) for item in raw]

    def list_size(self, name: str) -> int:
        return int(self._redis.llen(self._key_list(name)))

    def queue_values(self, name: str) -> list[Any]:
        raw = self._redis.lrange(self._key_queue(name), 0, -1)
        return [self._decode(item) for item in raw]

    def queue_size(self, name: str) -> int:
        return int(self._redis.llen(self._key_queue(name)))

    def queue_peek(self, name: str) -> Any:
        return self._decode(self._redis.lindex(self._key_queue(name), 0))

    # ------------------------------------------------------------------ #
    # Snapshot API
    # ------------------------------------------------------------------ #

    def create_snapshot(self) -> dict[str, Any]:
        """
        Build full snapshot of map/list/queue state from Redis.
        """
        maps: dict[str, Any] = {}
        lists: dict[str, Any] = {}
        queues: dict[str, Any] = {}

        for raw_name in self._redis.smembers(self._idx_maps()):
            name = self._decode_text(raw_name)
            maps[name] = self.map_items(name)
        for raw_name in self._redis.smembers(self._idx_lists()):
            name = self._decode_text(raw_name)
            lists[name] = self.list_values(name)
        for raw_name in self._redis.smembers(self._idx_queues()):
            name = self._decode_text(raw_name)
            queues[name] = self.queue_values(name)
        return {"maps": maps, "lists": lists, "queues": queues}

    def load_snapshot(self, snapshot: dict[str, Any]) -> None:
        """
        Replace all Redis-backed collection state from snapshot payload.
        """
        maps = dict(snapshot.get("maps", {}))
        lists = dict(snapshot.get("lists", {}))
        queues = dict(snapshot.get("queues", {}))

        with self._lock:
            # Clear existing keys/index sets under this namespace.
            keys = []
            for pattern in (
                f"{self.config.namespace}:map:*",
                f"{self.config.namespace}:list:*",
                f"{self.config.namespace}:queue:*",
            ):
                keys.extend(list(self._redis.scan_iter(match=pattern)))
            if keys:
                self._redis.delete(*keys)
            self._redis.delete(self._idx_maps(), self._idx_lists(), self._idx_queues())

            # Restore map/list/queue state.
            for name, mapping in maps.items():
                map_name = str(name)
                for key, value in dict(mapping).items():
                    self.apply_mutation(
                        {
                            "collection": "map",
                            "name": map_name,
                            "action": "put",
                            "key": str(key),
                            "value": value,
                        }
                    )
            for name, values in lists.items():
                list_name = str(name)
                self.apply_mutation(
                    {
                        "collection": "list",
                        "name": list_name,
                        "action": "extend",
                        "values": list(values),
                    }
                )
            for name, values in queues.items():
                queue_name = str(name)
                for value in list(values):
                    self.apply_mutation(
                        {
                            "collection": "queue",
                            "name": queue_name,
                            "action": "offer",
                            "value": value,
                        }
                    )
