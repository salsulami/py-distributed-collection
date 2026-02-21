"""
Thread-safe in-memory storage engine used by cluster nodes.

The store owns the authoritative local state for distributed collections.
Mutations are applied through a single ``apply_mutation`` path so local writes
and replicated remote writes follow identical logic.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from threading import RLock
from typing import Any

from .exceptions import UnsupportedOperationError


class ClusterDataStore:
    """
    Concurrent storage backing distributed map/list/queue abstractions.

    Notes
    -----
    * Topic messages are transient and therefore not persisted here.
    * The store performs defensive deep copies for read snapshots to protect
      internal state from accidental mutation by caller code.
    """

    is_centralized = False

    def __init__(self) -> None:
        """Create empty collection containers and initialize lock state."""
        self._maps: dict[str, dict[str, Any]] = {}
        self._lists: dict[str, list[Any]] = {}
        self._queues: dict[str, deque[Any]] = {}
        self._lock = RLock()

    def apply_mutation(self, mutation: dict[str, Any]) -> Any:
        """
        Apply one collection mutation to the local state.

        Parameters
        ----------
        mutation:
            Dictionary with at least ``collection``, ``name``, and ``action``.

        Returns
        -------
        Any
            Action-specific return value (for example removed item).
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

    def _apply_map_mutation(self, name: str, action: str, mutation: dict[str, Any]) -> Any:
        mapping = self._maps.setdefault(name, {})
        if action == "put":
            key = str(mutation["key"])
            existed = key in mapping
            previous = mapping.get(key)
            mapping[key] = mutation["value"]
            return {"existed": existed, "previous": previous}
        if action in {"remove", "expire_key"}:
            key = str(mutation["key"])
            if key in mapping:
                return {"removed": True, "value": mapping.pop(key)}
            return {"removed": False, "value": None}
        if action == "clear":
            removed_items = [{"key": key, "value": value} for key, value in mapping.items()]
            mapping.clear()
            return {"removed_items": removed_items}
        raise UnsupportedOperationError(f"Unknown map action: {action!r}")

    def _apply_list_mutation(self, name: str, action: str, mutation: dict[str, Any]) -> Any:
        target = self._lists.setdefault(name, [])
        if action == "append":
            target.append(mutation["value"])
            return {"added": True, "index": len(target) - 1, "value": mutation["value"]}
        if action == "extend":
            values = list(mutation["values"])
            start_index = len(target)
            target.extend(values)
            return {
                "added_items": [
                    {"index": start_index + index, "value": value}
                    for index, value in enumerate(values)
                ]
            }
        if action == "set":
            index = int(mutation["index"])
            previous = target[index]
            target[index] = mutation["value"]
            return {"updated": True, "index": index, "previous": previous, "value": mutation["value"]}
        if action == "pop":
            if "index" in mutation and mutation["index"] is not None:
                index = int(mutation["index"])
                resolved_index = index if index >= 0 else len(target) + index
                value = target.pop(index)
                return {"removed": True, "index": resolved_index, "value": value}
            resolved_index = len(target) - 1
            value = target.pop()
            return {"removed": True, "index": resolved_index, "value": value}
        if action == "remove_value":
            value = mutation["value"]
            try:
                index = target.index(value)
                target.pop(index)
                return {"removed": True, "index": index, "value": value}
            except ValueError:
                return {"removed": False, "index": None, "value": value}
        if action == "expire_index":
            index = int(mutation["index"])
            resolved_index = index if index >= 0 else len(target) + index
            if resolved_index < 0 or resolved_index >= len(target):
                return {"removed": False, "index": None, "value": None}
            value = target.pop(resolved_index)
            return {"removed": True, "index": resolved_index, "value": value}
        if action == "clear":
            removed_items = [{"index": index, "value": value} for index, value in enumerate(target)]
            target.clear()
            return {"removed_items": removed_items}
        raise UnsupportedOperationError(f"Unknown list action: {action!r}")

    def _apply_queue_mutation(self, name: str, action: str, mutation: dict[str, Any]) -> Any:
        target = self._queues.setdefault(name, deque())
        if action == "offer":
            target.append(mutation["value"])
            return {"added": True, "index": len(target) - 1, "value": mutation["value"]}
        if action == "poll":
            if not target:
                return {"removed": False, "index": None, "value": None}
            value = target.popleft()
            return {"removed": True, "index": 0, "value": value}
        if action == "expire_index":
            index = int(mutation["index"])
            resolved_index = index if index >= 0 else len(target) + index
            if resolved_index < 0 or resolved_index >= len(target):
                return {"removed": False, "index": None, "value": None}
            values = list(target)
            value = values.pop(resolved_index)
            self._queues[name] = deque(values)
            return {"removed": True, "index": resolved_index, "value": value}
        if action == "clear":
            removed_items = [{"index": index, "value": value} for index, value in enumerate(target)]
            target.clear()
            return {"removed_items": removed_items}
        raise UnsupportedOperationError(f"Unknown queue action: {action!r}")

    def map_get(self, name: str, key: str, default: Any = None) -> Any:
        """
        Read one map value.

        Returns a deep copy to prevent callers from mutating internal state.
        """
        with self._lock:
            value = self._maps.get(name, {}).get(key, default)
            return deepcopy(value)

    def map_items(self, name: str) -> dict[str, Any]:
        """Return a full copy of a named map."""
        with self._lock:
            return deepcopy(self._maps.get(name, {}))

    def map_contains(self, name: str, key: str) -> bool:
        """Return ``True`` when map contains the provided key."""
        with self._lock:
            return key in self._maps.get(name, {})

    def map_size(self, name: str) -> int:
        """Return map entry count."""
        with self._lock:
            return len(self._maps.get(name, {}))

    def list_values(self, name: str) -> list[Any]:
        """Return a full copy of a distributed list."""
        with self._lock:
            return deepcopy(self._lists.get(name, []))

    def list_size(self, name: str) -> int:
        """Return list length."""
        with self._lock:
            return len(self._lists.get(name, []))

    def queue_values(self, name: str) -> list[Any]:
        """Return queue values from head to tail as a list copy."""
        with self._lock:
            return deepcopy(list(self._queues.get(name, deque())))

    def queue_size(self, name: str) -> int:
        """Return queue length."""
        with self._lock:
            return len(self._queues.get(name, deque()))

    def queue_peek(self, name: str) -> Any:
        """Return the current queue head without removal."""
        with self._lock:
            target = self._queues.get(name)
            if not target:
                return None
            return deepcopy(target[0])

    def create_snapshot(self) -> dict[str, Any]:
        """
        Build a full serializable snapshot of all stored collections.

        Returns
        -------
        dict[str, Any]
            Dictionary with ``maps``, ``lists``, and ``queues`` sections.
        """
        with self._lock:
            return {
                "maps": deepcopy(self._maps),
                "lists": deepcopy(self._lists),
                "queues": {name: deepcopy(list(values)) for name, values in self._queues.items()},
            }

    def load_snapshot(self, snapshot: dict[str, Any]) -> None:
        """
        Replace local store state with an externally provided snapshot.

        Parameters
        ----------
        snapshot:
            Dictionary produced by :meth:`create_snapshot`.
        """
        with self._lock:
            maps = snapshot.get("maps", {})
            lists = snapshot.get("lists", {})
            queues = snapshot.get("queues", {})
            self._maps = deepcopy(dict(maps))
            self._lists = deepcopy(dict(lists))
            self._queues = {
                str(name): deque(values) for name, values in dict(queues).items()
            }
