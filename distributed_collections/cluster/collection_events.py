from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any


class ClusterCollectionEventsMixin:
    """
    Collection item listener registration and event dispatch.
    """

    def _collection_add_listener(
        self,
        collection: str,
        name: str,
        callback: Callable[[dict[str, Any]], None],
    ) -> str:
        """Register an item listener for one collection component."""
        subscription_id = uuid.uuid4().hex
        key = (str(collection), str(name))
        with self._collection_listener_lock:
            listeners = self._collection_listeners.setdefault(key, {})
            listeners[subscription_id] = callback
        return subscription_id

    def _collection_remove_listener(self, collection: str, name: str, subscription_id: str) -> bool:
        """Remove one collection listener by subscription ID."""
        key = (str(collection), str(name))
        with self._collection_listener_lock:
            listeners = self._collection_listeners.get(key)
            if not listeners:
                return False
            removed = listeners.pop(subscription_id, None) is not None
            if not listeners:
                self._collection_listeners.pop(key, None)
            return removed

    def _emit_collection_event(self, event: dict[str, Any]) -> None:
        """Dispatch one event payload to local listeners."""
        collection = str(event.get("collection", ""))
        name = str(event.get("name", ""))
        key = (collection, name)
        with self._collection_listener_lock:
            callbacks = list(self._collection_listeners.get(key, {}).values())
        for callback in callbacks:
            try:
                callback(event)
            except Exception:
                continue

    def _emit_collection_events_for_mutation(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        payload: dict[str, Any],
        result: Any,
        source: str,
    ) -> None:
        """
        Build and emit collection item events for one applied mutation.
        """
        if source not in {"local", "remote"}:
            return
        if collection not in {"map", "list", "queue"}:
            return

        event_base = {
            "collection": collection,
            "name": name,
            "source": source,
            "ts_ms": int(time.time() * 1000),
        }
        events: list[dict[str, Any]] = []
        result_payload = result if isinstance(result, dict) else {}

        if collection == "map":
            key = str(payload.get("key", ""))
            if action == "put":
                existed = bool(result_payload.get("existed"))
                event = {
                    **event_base,
                    "event": "updated" if existed else "added",
                    "key": key,
                    "value": payload.get("value"),
                }
                if existed:
                    event["old_value"] = result_payload.get("previous")
                events.append(event)
            elif action == "remove":
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "deleted",
                            "key": key,
                            "old_value": result_payload.get("value"),
                        }
                    )
            elif action == "expire_key":
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "evicted",
                            "key": key,
                            "old_value": result_payload.get("value"),
                            "reason": "ttl",
                        }
                    )
            elif action == "clear":
                removed_items = result_payload.get("removed_items", [])
                if isinstance(removed_items, list):
                    for item in removed_items:
                        if not isinstance(item, dict):
                            continue
                        events.append(
                            {
                                **event_base,
                                "event": "deleted",
                                "key": str(item.get("key", "")),
                                "old_value": item.get("value"),
                            }
                        )

        elif collection == "list":
            if action == "append":
                events.append(
                    {
                        **event_base,
                        "event": "added",
                        "index": result_payload.get("index"),
                        "value": payload.get("value"),
                    }
                )
            elif action == "extend":
                added_items = result_payload.get("added_items", [])
                if isinstance(added_items, list):
                    for item in added_items:
                        if not isinstance(item, dict):
                            continue
                        events.append(
                            {
                                **event_base,
                                "event": "added",
                                "index": item.get("index"),
                                "value": item.get("value"),
                            }
                        )
            elif action == "set":
                if bool(result_payload.get("updated")):
                    events.append(
                        {
                            **event_base,
                            "event": "updated",
                            "index": result_payload.get("index"),
                            "value": payload.get("value"),
                            "old_value": result_payload.get("previous"),
                        }
                    )
            elif action in {"pop", "remove_value"}:
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "deleted",
                            "index": result_payload.get("index"),
                            "old_value": result_payload.get("value"),
                        }
                    )
            elif action == "expire_index":
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "evicted",
                            "index": result_payload.get("index"),
                            "old_value": result_payload.get("value"),
                            "reason": "ttl",
                        }
                    )
            elif action == "clear":
                removed_items = result_payload.get("removed_items", [])
                if isinstance(removed_items, list):
                    for item in removed_items:
                        if not isinstance(item, dict):
                            continue
                        events.append(
                            {
                                **event_base,
                                "event": "deleted",
                                "index": item.get("index"),
                                "old_value": item.get("value"),
                            }
                        )

        elif collection == "queue":
            if action == "offer":
                events.append(
                    {
                        **event_base,
                        "event": "added",
                        "index": result_payload.get("index"),
                        "value": payload.get("value"),
                    }
                )
            elif action == "poll":
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "deleted",
                            "index": result_payload.get("index"),
                            "old_value": result_payload.get("value"),
                        }
                    )
            elif action == "expire_index":
                if bool(result_payload.get("removed")):
                    events.append(
                        {
                            **event_base,
                            "event": "evicted",
                            "index": result_payload.get("index"),
                            "old_value": result_payload.get("value"),
                            "reason": "ttl",
                        }
                    )
            elif action == "clear":
                removed_items = result_payload.get("removed_items", [])
                if isinstance(removed_items, list):
                    for item in removed_items:
                        if not isinstance(item, dict):
                            continue
                        events.append(
                            {
                                **event_base,
                                "event": "deleted",
                                "index": item.get("index"),
                                "old_value": item.get("value"),
                            }
                        )

        for event in events:
            self._emit_collection_event(event)
