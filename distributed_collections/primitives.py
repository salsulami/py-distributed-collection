"""
User-facing distributed collection wrappers.

These classes expose familiar collection-style APIs while delegating all
mutating operations to :class:`distributed_collections.cluster.ClusterNode`,
which performs replication to other nodes.
"""

from __future__ import annotations

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
