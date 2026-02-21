"""
User-facing distributed collection wrappers.

These classes expose familiar collection-style APIs while delegating all
mutating operations to :class:`distributed_collections.cluster.ClusterNode`,
which performs replication to other nodes.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing-only import
    from .cluster import ClusterNode


class DistributedMap(MutableMapping[str, Any]):
    """
    Distributed key/value map replicated across cluster members.

    Mutating operations (`put`, `remove`, `clear`, assignment/deletion syntax)
    are replicated via the owning cluster node.
    """

    def __init__(self, cluster: "ClusterNode", name: str) -> None:
        self._cluster = cluster
        self._name = name

    @property
    def name(self) -> str:
        """Return the logical map name."""
        return self._name

    def put(self, key: str, value: Any) -> Any:
        """
        Insert or update a key and replicate the operation.

        Returns
        -------
        Any
            Previous value for the key, or ``None`` if absent.
        """
        return self._cluster._map_put(self._name, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        """Return key value if present, otherwise ``default``."""
        return self._cluster._map_get(self._name, key, default)

    def remove(self, key: str) -> Any:
        """
        Remove a key from the distributed map.

        Returns
        -------
        Any
            Removed value, or ``None`` if key did not exist.
        """
        return self._cluster._map_remove(self._name, key)

    def clear(self) -> None:
        """Delete all keys from the map on all connected cluster nodes."""
        self._cluster._map_clear(self._name)

    def items_dict(self) -> dict[str, Any]:
        """Return a full dictionary snapshot of the map."""
        return self._cluster._map_items(self._name)

    def set_ttl(self, ttl_seconds: float | None) -> None:
        """
        Configure component-level TTL for all entries in this map.

        Passing ``None`` disables TTL for this map.
        """
        self._cluster._set_component_ttl("map", self._name, ttl_seconds)

    def ttl_seconds(self) -> float | None:
        """Return configured map TTL in seconds, or ``None`` when disabled."""
        return self._cluster._get_component_ttl("map", self._name)

    def add_listener(self, callback: Callable[[dict[str, Any]], None]) -> str:
        """
        Register a callback for map item lifecycle events.

        Event payload includes ``event`` (added/updated/deleted/evicted), the
        map ``name``, and mutation-specific fields like ``key`` and ``value``.
        """
        return self._cluster._collection_add_listener("map", self._name, callback)

    def remove_listener(self, subscription_id: str) -> bool:
        """Unregister a previously added map event listener."""
        return self._cluster._collection_remove_listener("map", self._name, subscription_id)

    def __getitem__(self, key: str) -> Any:
        value = self.get(key)
        if value is None and not self._cluster._map_contains(self._name, key):
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        self.put(key, value)

    def __delitem__(self, key: str) -> None:
        if not self._cluster._map_contains(self._name, key):
            raise KeyError(key)
        self.remove(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.items_dict())

    def __len__(self) -> int:
        return self._cluster._map_size(self._name)


class DistributedList:
    """
    Distributed ordered list replicated across cluster members.

    List methods mirror Python list behavior where possible.
    """

    def __init__(self, cluster: "ClusterNode", name: str) -> None:
        self._cluster = cluster
        self._name = name

    @property
    def name(self) -> str:
        """Return the logical list name."""
        return self._name

    def append(self, value: Any) -> None:
        """Append a value to the end of the distributed list."""
        self._cluster._list_append(self._name, value)

    def extend(self, values: list[Any]) -> None:
        """Append all values from an iterable-like list."""
        self._cluster._list_extend(self._name, list(values))

    def pop(self, index: int | None = None) -> Any:
        """
        Remove and return an item from the list.

        Parameters
        ----------
        index:
            Optional index. If omitted, removes the last element.
        """
        return self._cluster._list_pop(self._name, index)

    def remove(self, value: Any) -> bool:
        """
        Remove first matching value from the list.

        Returns ``True`` when an item was removed, otherwise ``False``.
        """
        return bool(self._cluster._list_remove_value(self._name, value))

    def clear(self) -> None:
        """Remove all items from the distributed list."""
        self._cluster._list_clear(self._name)

    def values(self) -> list[Any]:
        """Return a snapshot copy of the current list values."""
        return self._cluster._list_values(self._name)

    def set_ttl(self, ttl_seconds: float | None) -> None:
        """
        Configure component-level TTL for all items in this list.

        Passing ``None`` disables TTL for this list.
        """
        self._cluster._set_component_ttl("list", self._name, ttl_seconds)

    def ttl_seconds(self) -> float | None:
        """Return configured list TTL in seconds, or ``None`` when disabled."""
        return self._cluster._get_component_ttl("list", self._name)

    def add_listener(self, callback: Callable[[dict[str, Any]], None]) -> str:
        """
        Register a callback for list item lifecycle events.

        Event payload includes ``event`` (added/updated/deleted/evicted), list
        ``name``, and fields like ``index`` and ``value``.
        """
        return self._cluster._collection_add_listener("list", self._name, callback)

    def remove_listener(self, subscription_id: str) -> bool:
        """Unregister a previously added list event listener."""
        return self._cluster._collection_remove_listener("list", self._name, subscription_id)

    def __getitem__(self, index: int) -> Any:
        return self.values()[index]

    def __setitem__(self, index: int, value: Any) -> None:
        self._cluster._list_set(self._name, index, value)

    def __len__(self) -> int:
        return self._cluster._list_size(self._name)

    def __iter__(self) -> Iterator[Any]:
        return iter(self.values())


class DistributedQueue:
    """
    Distributed FIFO queue replicated across cluster members.

    Queue operations follow common message queue terminology:
    ``offer`` to enqueue and ``poll`` to dequeue.
    """

    def __init__(self, cluster: "ClusterNode", name: str) -> None:
        self._cluster = cluster
        self._name = name

    @property
    def name(self) -> str:
        """Return the logical queue name."""
        return self._name

    def offer(self, value: Any) -> None:
        """Enqueue one item at the queue tail and replicate the operation."""
        self._cluster._queue_offer(self._name, value)

    def poll(self) -> Any:
        """Dequeue and return queue head, or ``None`` when queue is empty."""
        return self._cluster._queue_poll(self._name)

    def peek(self) -> Any:
        """Return queue head without dequeuing, or ``None`` if empty."""
        return self._cluster._queue_peek(self._name)

    def clear(self) -> None:
        """Remove all queued values on all cluster peers."""
        self._cluster._queue_clear(self._name)

    def values(self) -> list[Any]:
        """Return queue content from head to tail."""
        return self._cluster._queue_values(self._name)

    def set_ttl(self, ttl_seconds: float | None) -> None:
        """
        Configure component-level TTL for all queue items.

        Passing ``None`` disables TTL for this queue.
        """
        self._cluster._set_component_ttl("queue", self._name, ttl_seconds)

    def ttl_seconds(self) -> float | None:
        """Return configured queue TTL in seconds, or ``None`` when disabled."""
        return self._cluster._get_component_ttl("queue", self._name)

    def add_listener(self, callback: Callable[[dict[str, Any]], None]) -> str:
        """
        Register a callback for queue item lifecycle events.

        Event payload includes ``event`` (added/updated/deleted/evicted), queue
        ``name``, and fields like ``index`` and ``value``.
        """
        return self._cluster._collection_add_listener("queue", self._name, callback)

    def remove_listener(self, subscription_id: str) -> bool:
        """Unregister a previously added queue event listener."""
        return self._cluster._collection_remove_listener("queue", self._name, subscription_id)

    def size(self) -> int:
        """Return queue length."""
        return self._cluster._queue_size(self._name)

    def __len__(self) -> int:
        return self.size()


class DistributedTopic:
    """
    Distributed pub/sub topic.

    Topic messages are not persisted in the data store; they are forwarded to
    live subscribers on each node.
    """

    def __init__(self, cluster: "ClusterNode", name: str) -> None:
        self._cluster = cluster
        self._name = name

    @property
    def name(self) -> str:
        """Return the logical topic name."""
        return self._name

    def subscribe(self, callback: Callable[[Any], None]) -> str:
        """
        Register a callback for future published messages.

        Returns
        -------
        str
            Subscription ID used to unsubscribe later.
        """
        return self._cluster._topic_subscribe(self._name, callback)

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Remove a previously registered callback.

        Returns ``True`` when removed, otherwise ``False``.
        """
        return self._cluster._topic_unsubscribe(self._name, subscription_id)

    def publish(self, message: Any) -> None:
        """Publish a message to subscribers on all connected nodes."""
        self._cluster._topic_publish(self._name, message)


class DistributedLock:
    """
    CP-backed distributed lock with lease and fencing-token semantics.

    The lock is coordinated via the cluster CP subsystem and committed through
    the same ordered replication path as other linearizable operations.
    """

    def __init__(self, cluster: "ClusterNode", name: str) -> None:
        self._cluster = cluster
        self._name = name
        self._owner_id = uuid.uuid4().hex
        self._token: str | None = None
        self._fencing_token: int | None = None

    @property
    def name(self) -> str:
        """Return the logical lock name."""
        return self._name

    @property
    def fencing_token(self) -> int | None:
        """
        Return the latest fencing token acquired by this handle.

        The token is monotonic per lock name and useful for fencing external
        side-effects when lock ownership changes.
        """
        return self._fencing_token

    def acquire(
        self,
        *,
        blocking: bool = True,
        timeout_seconds: float | None = None,
        lease_seconds: float | None = None,
    ) -> bool:
        """
        Attempt to acquire the distributed lock.

        Parameters
        ----------
        blocking:
            When true, retry until acquired or timeout.
        timeout_seconds:
            Maximum wait duration when ``blocking`` is true.
        lease_seconds:
            Lock lease duration; defaults to CP subsystem configuration.
        """
        result = self._cluster._cp_lock_acquire(
            self._name,
            owner_id=self._owner_id,
            blocking=blocking,
            timeout_seconds=timeout_seconds,
            lease_seconds=lease_seconds,
            current_token=self._token,
        )
        if not result.get("acquired", False):
            return False
        self._token = str(result["token"])
        self._fencing_token = int(result["fencing_token"])
        return True

    def try_acquire(self, *, lease_seconds: float | None = None) -> bool:
        """Try to acquire lock once without blocking."""
        return self.acquire(blocking=False, lease_seconds=lease_seconds)

    def refresh(self, *, lease_seconds: float | None = None) -> bool:
        """
        Extend lease for the lock currently owned by this handle.
        """
        if self._token is None:
            return False
        return self._cluster._cp_lock_refresh(
            self._name,
            owner_id=self._owner_id,
            token=self._token,
            lease_seconds=lease_seconds,
        )

    def release(self) -> bool:
        """Release lock ownership for this handle."""
        if self._token is None:
            return False
        released = self._cluster._cp_lock_release(
            self._name,
            owner_id=self._owner_id,
            token=self._token,
        )
        if released:
            self._token = None
            self._fencing_token = None
        return released

    def is_locked(self) -> bool:
        """Return ``True`` when the lock is currently owned by any session."""
        return self._cluster._cp_lock_is_locked(self._name)

    def owner(self) -> str | None:
        """Return current owner session ID, or ``None`` when lock is free."""
        return self._cluster._cp_lock_owner(self._name)

    def __enter__(self) -> "DistributedLock":
        if not self.acquire(blocking=True):
            raise TimeoutError(f"Failed to acquire distributed lock {self._name!r}.")
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self.release()
        return False
