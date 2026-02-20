from __future__ import annotations

from typing import Any


class ClusterPrimitiveAdapterMixin:
    """
    Primitive adapter methods used by distributed wrappers.
    """

    def _map_put(self, map_name: str, key: str, value: Any) -> Any:
        return self._submit_local_operation(
            collection="map",
            name=map_name,
            action="put",
            values={"key": key, "value": value},
        )

    def _map_get(self, map_name: str, key: str, default: Any = None) -> Any:
        return self._store.map_get(map_name, key, default)

    def _map_remove(self, map_name: str, key: str) -> Any:
        return self._submit_local_operation(
            collection="map",
            name=map_name,
            action="remove",
            values={"key": key},
        )

    def _map_clear(self, map_name: str) -> None:
        self._submit_local_operation(
            collection="map",
            name=map_name,
            action="clear",
        )

    def _map_items(self, map_name: str) -> dict[str, Any]:
        return self._store.map_items(map_name)

    def _map_size(self, map_name: str) -> int:
        return self._store.map_size(map_name)

    def _map_contains(self, map_name: str, key: str) -> bool:
        return self._store.map_contains(map_name, key)

    def _list_append(self, list_name: str, value: Any) -> None:
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="append",
            values={"value": value},
        )

    def _list_extend(self, list_name: str, values: list[Any]) -> None:
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="extend",
            values={"values": values},
        )

    def _list_set(self, list_name: str, index: int, value: Any) -> Any:
        return self._submit_local_operation(
            collection="list",
            name=list_name,
            action="set",
            values={"index": index, "value": value},
        )

    def _list_pop(self, list_name: str, index: int | None = None) -> Any:
        return self._submit_local_operation(
            collection="list",
            name=list_name,
            action="pop",
            values={"index": index},
        )

    def _list_remove_value(self, list_name: str, value: Any) -> bool:
        return bool(
            self._submit_local_operation(
                collection="list",
                name=list_name,
                action="remove_value",
                values={"value": value},
            )
        )

    def _list_clear(self, list_name: str) -> None:
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="clear",
        )

    def _list_values(self, list_name: str) -> list[Any]:
        return self._store.list_values(list_name)

    def _list_size(self, list_name: str) -> int:
        return self._store.list_size(list_name)

    def _queue_offer(self, queue_name: str, value: Any) -> None:
        self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="offer",
            values={"value": value},
        )

    def _queue_poll(self, queue_name: str) -> Any:
        return self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="poll",
        )

    def _queue_clear(self, queue_name: str) -> None:
        self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="clear",
        )

    def _queue_peek(self, queue_name: str) -> Any:
        return self._store.queue_peek(queue_name)

    def _queue_values(self, queue_name: str) -> list[Any]:
        return self._store.queue_values(queue_name)

    def _queue_size(self, queue_name: str) -> int:
        return self._store.queue_size(queue_name)
