from __future__ import annotations

from ..primitives import (
    DistributedList,
    DistributedLock,
    DistributedMap,
    DistributedQueue,
    DistributedTopic,
)


class ClusterHandleMixin:
    """
    Public distributed collection handle factories.
    """

    def get_map(self, name: str) -> DistributedMap:
        """Return distributed map handle by logical name."""
        with self._handle_lock:
            handle = self._map_handles.get(name)
            if handle is None:
                handle = DistributedMap(self, name)
                self._map_handles[name] = handle
            return handle

    def get_list(self, name: str) -> DistributedList:
        """Return distributed list handle by logical name."""
        with self._handle_lock:
            handle = self._list_handles.get(name)
            if handle is None:
                handle = DistributedList(self, name)
                self._list_handles[name] = handle
            return handle

    def get_queue(self, name: str) -> DistributedQueue:
        """Return distributed queue handle by logical name."""
        with self._handle_lock:
            handle = self._queue_handles.get(name)
            if handle is None:
                handle = DistributedQueue(self, name)
                self._queue_handles[name] = handle
            return handle

    def get_topic(self, name: str) -> DistributedTopic:
        """Return distributed topic handle by logical name."""
        with self._handle_lock:
            handle = self._topic_handles.get(name)
            if handle is None:
                handle = DistributedTopic(self, name)
                self._topic_handles[name] = handle
            return handle

    def get_lock(self, name: str) -> DistributedLock:
        """Return CP-backed distributed lock handle by logical name."""
        with self._handle_lock:
            handle = self._lock_handles.get(name)
            if handle is None:
                handle = DistributedLock(self, name)
                self._lock_handles[name] = handle
            return handle
