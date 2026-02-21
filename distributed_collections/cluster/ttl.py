from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from typing import Any


class ClusterTTLMixin:
    """
    Component-level TTL configuration, tracking, and eviction worker.
    """

    def _normalize_component_ttl_seconds(self, ttl_seconds: float | None) -> float | None:
        if ttl_seconds is None:
            return None
        normalized = float(ttl_seconds)
        if normalized <= 0:
            raise ValueError("Component TTL must be > 0 seconds, or None to disable.")
        return normalized

    def _get_component_ttl(self, collection: str, name: str) -> float | None:
        """Return configured component TTL in seconds."""
        with self._ttl_lock:
            if collection == "map":
                return self._map_ttl_seconds.get(name)
            if collection == "list":
                return self._list_ttl_seconds.get(name)
            if collection == "queue":
                return self._queue_ttl_seconds.get(name)
        raise ValueError(f"Unsupported TTL collection type: {collection!r}")

    def _set_component_ttl(self, collection: str, name: str, ttl_seconds: float | None) -> None:
        """
        Set or clear component-level TTL for map/list/queue.
        """
        normalized = self._normalize_component_ttl_seconds(ttl_seconds)
        with self._ttl_lock:
            if collection == "map":
                if normalized is None:
                    self._map_ttl_seconds.pop(name, None)
                    self._ttl_map_entries.pop(name, None)
                else:
                    self._map_ttl_seconds[name] = normalized
            elif collection == "list":
                if normalized is None:
                    self._list_ttl_seconds.pop(name, None)
                    self._ttl_list_entries.pop(name, None)
                else:
                    self._list_ttl_seconds[name] = normalized
            elif collection == "queue":
                if normalized is None:
                    self._queue_ttl_seconds.pop(name, None)
                    self._ttl_queue_entries.pop(name, None)
                else:
                    self._queue_ttl_seconds[name] = normalized
            else:
                raise ValueError(f"Unsupported TTL collection type: {collection!r}")

        if normalized is not None:
            self._hydrate_component_ttl(collection=collection, name=name, ttl_seconds=normalized)

    def _component_ttl_ms(self, collection: str, name: str) -> int | None:
        ttl_seconds = self._get_component_ttl(collection, name)
        if ttl_seconds is None:
            return None
        return max(1, int(ttl_seconds * 1000))

    def _hydrate_component_ttl(self, *, collection: str, name: str, ttl_seconds: float) -> None:
        """
        Apply configured component TTL to all currently stored items.
        """
        ttl_ms = max(1, int(ttl_seconds * 1000))
        deadline_ms = int(time.time() * 1000) + ttl_ms

        if collection == "map":
            keys = [str(key) for key in self._store.map_items(name).keys()]
            with self._ttl_lock:
                self._ttl_map_entries[name] = {key: deadline_ms for key in keys}
            return

        if collection == "list":
            values = self._store.list_values(name)
            entries = [
                {"token": uuid.uuid4().hex, "expires_at_ms": deadline_ms} for _ in values
            ]
            with self._ttl_lock:
                self._ttl_list_entries[name] = entries
            return

        if collection == "queue":
            values = self._store.queue_values(name)
            entries = deque(
                {"token": uuid.uuid4().hex, "expires_at_ms": deadline_ms} for _ in values
            )
            with self._ttl_lock:
                self._ttl_queue_entries[name] = entries
            return

        raise ValueError(f"Unsupported TTL collection type: {collection!r}")

    def _hydrate_all_component_ttls(self) -> None:
        """
        Initialize TTL metadata from configured component TTL values.
        """
        with self._ttl_lock:
            map_ttls = dict(self._map_ttl_seconds)
            list_ttls = dict(self._list_ttl_seconds)
            queue_ttls = dict(self._queue_ttl_seconds)
            existing_map = set(self._ttl_map_entries)
            existing_list = set(self._ttl_list_entries)
            existing_queue = set(self._ttl_queue_entries)
        for name, ttl_seconds in map_ttls.items():
            if name in existing_map:
                continue
            self._hydrate_component_ttl(collection="map", name=name, ttl_seconds=ttl_seconds)
        for name, ttl_seconds in list_ttls.items():
            if name in existing_list:
                continue
            self._hydrate_component_ttl(collection="list", name=name, ttl_seconds=ttl_seconds)
        for name, ttl_seconds in queue_ttls.items():
            if name in existing_queue:
                continue
            self._hydrate_component_ttl(collection="queue", name=name, ttl_seconds=ttl_seconds)

    def _augment_operation_with_ttl(self, operation: dict[str, Any]) -> dict[str, Any]:
        """
        Attach TTL metadata to mutation operations that create/update items.
        """
        collection = str(operation.get("collection", ""))
        if collection not in {"map", "list", "queue"}:
            return operation
        name = str(operation.get("name", ""))
        action = str(operation.get("action", ""))
        ttl_ms = self._component_ttl_ms(collection, name)
        if ttl_ms is None:
            return operation

        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + ttl_ms
        enriched = dict(operation)

        if collection == "map" and action == "put":
            enriched["ttl_ms"] = ttl_ms
            enriched["ttl_expires_at_ms"] = expires_at_ms
            return enriched

        if collection == "list":
            if action == "append":
                enriched["ttl_ms"] = ttl_ms
                enriched["ttl_expires_at_ms"] = expires_at_ms
                enriched["ttl_token"] = uuid.uuid4().hex
            elif action == "extend":
                values = list(enriched.get("values", []))
                enriched["ttl_ms"] = ttl_ms
                enriched["ttl_expires_at_ms"] = expires_at_ms
                enriched["ttl_tokens"] = [uuid.uuid4().hex for _ in values]
            elif action == "set":
                enriched["ttl_ms"] = ttl_ms
                enriched["ttl_expires_at_ms"] = expires_at_ms
                enriched["ttl_token"] = uuid.uuid4().hex
            return enriched

        if collection == "queue" and action == "offer":
            enriched["ttl_ms"] = ttl_ms
            enriched["ttl_expires_at_ms"] = expires_at_ms
            enriched["ttl_token"] = uuid.uuid4().hex
            return enriched

        return enriched

    def _ttl_eviction_guard_allows(self, payload: dict[str, Any]) -> bool:
        """
        Return true when an eviction mutation still matches tracked TTL metadata.
        """
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        name = str(payload.get("name", ""))
        if collection == "map" and action == "expire_key":
            key = str(payload.get("key", ""))
            expected_expires_at = int(payload.get("expected_expires_at_ms", 0))
            with self._ttl_lock:
                current = self._ttl_map_entries.get(name, {}).get(key)
            return current is not None and int(current) == expected_expires_at

        if collection in {"list", "queue"} and action == "expire_index":
            expected_expires_at = int(payload.get("expected_expires_at_ms", 0))
            token = str(payload.get("token", ""))
            with self._ttl_lock:
                if collection == "list":
                    entries = self._ttl_list_entries.get(name, [])
                    as_list = list(entries)
                else:
                    entries = self._ttl_queue_entries.get(name, deque())
                    as_list = list(entries)

                resolved_index: int | None = None
                if token:
                    for index, entry in enumerate(as_list):
                        if isinstance(entry, dict) and str(entry.get("token", "")) == token:
                            resolved_index = index
                            break
                if resolved_index is None:
                    index_hint = int(payload.get("index", -1))
                    if index_hint < 0:
                        index_hint += len(as_list)
                    if 0 <= index_hint < len(as_list):
                        resolved_index = index_hint
                if resolved_index is None:
                    return False
                entry = as_list[resolved_index]
                if not isinstance(entry, dict):
                    return False
                if int(entry.get("expires_at_ms", 0)) != expected_expires_at:
                    return False
                payload["index"] = resolved_index
                return True

        return True

    def _apply_ttl_metadata(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        payload: dict[str, Any],
        result: Any,
    ) -> None:
        """
        Update local TTL metadata after a successful collection mutation.
        """
        if collection not in {"map", "list", "queue"}:
            return
        result_payload = result if isinstance(result, dict) else {}

        with self._ttl_lock:
            if collection == "map":
                if action == "put":
                    key = str(payload.get("key", ""))
                    expires_at = payload.get("ttl_expires_at_ms")
                    entries = self._ttl_map_entries.setdefault(name, {})
                    if expires_at is None:
                        entries.pop(key, None)
                    else:
                        entries[key] = int(expires_at)
                    if not entries:
                        self._ttl_map_entries.pop(name, None)
                    return
                if action in {"remove", "expire_key"}:
                    if bool(result_payload.get("removed")):
                        key = str(payload.get("key", ""))
                        entries = self._ttl_map_entries.get(name, {})
                        entries.pop(key, None)
                        if not entries:
                            self._ttl_map_entries.pop(name, None)
                    return
                if action == "clear":
                    self._ttl_map_entries.pop(name, None)
                    return

            if collection == "list":
                entries = self._ttl_list_entries.get(name)
                incoming_ttl = "ttl_expires_at_ms" in payload
                if entries is None and not incoming_ttl and action not in {"clear"}:
                    return
                if entries is None:
                    entries = []
                    self._ttl_list_entries[name] = entries

                if action == "append":
                    entries.append(self._build_ttl_sequence_entry(payload))
                    return
                if action == "extend":
                    values = list(payload.get("values", []))
                    tokens = payload.get("ttl_tokens")
                    expires_at = payload.get("ttl_expires_at_ms")
                    for index, _ in enumerate(values):
                        if (
                            isinstance(tokens, list)
                            and index < len(tokens)
                            and expires_at is not None
                        ):
                            token = str(tokens[index])
                            if token:
                                entries.append({"token": token, "expires_at_ms": int(expires_at)})
                                continue
                        entries.append(None)
                    return
                if action == "set":
                    index = int(payload.get("index", 0))
                    resolved_index = self._resolve_index(index, len(entries))
                    if resolved_index is None:
                        return
                    entries[resolved_index] = self._build_ttl_sequence_entry(payload)
                    return
                if action in {"pop", "remove_value", "expire_index"}:
                    if not bool(result_payload.get("removed")):
                        return
                    removed_index = result_payload.get("index")
                    if removed_index is None:
                        return
                    resolved_index = self._resolve_index(int(removed_index), len(entries))
                    if resolved_index is None:
                        return
                    entries.pop(resolved_index)
                    if not entries and name not in self._list_ttl_seconds:
                        self._ttl_list_entries.pop(name, None)
                    return
                if action == "clear":
                    self._ttl_list_entries.pop(name, None)
                    return

            if collection == "queue":
                entries = self._ttl_queue_entries.get(name)
                incoming_ttl = "ttl_expires_at_ms" in payload
                if entries is None and not incoming_ttl and action not in {"clear"}:
                    return
                if entries is None:
                    entries = deque()
                    self._ttl_queue_entries[name] = entries

                if action == "offer":
                    entries.append(self._build_ttl_sequence_entry(payload))
                    return
                if action in {"poll", "expire_index"}:
                    if not bool(result_payload.get("removed")):
                        return
                    removed_index = result_payload.get("index")
                    if removed_index is None:
                        return
                    resolved_index = self._resolve_index(int(removed_index), len(entries))
                    if resolved_index is None:
                        return
                    items = list(entries)
                    items.pop(resolved_index)
                    self._ttl_queue_entries[name] = deque(items)
                    if not items and name not in self._queue_ttl_seconds:
                        self._ttl_queue_entries.pop(name, None)
                    return
                if action == "clear":
                    self._ttl_queue_entries.pop(name, None)
                    return

    def _build_ttl_sequence_entry(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        token = payload.get("ttl_token")
        expires_at = payload.get("ttl_expires_at_ms")
        if token is None or expires_at is None:
            return None
        token_text = str(token)
        if not token_text:
            return None
        return {"token": token_text, "expires_at_ms": int(expires_at)}

    def _resolve_index(self, index: int, length: int) -> int | None:
        resolved = index if index >= 0 else length + index
        if resolved < 0 or resolved >= length:
            return None
        return resolved

    def _start_ttl_worker(self) -> None:
        """Start background TTL eviction worker."""
        if self._ttl_thread is not None and self._ttl_thread.is_alive():
            return
        self._ttl_stop.clear()
        self._ttl_thread = threading.Thread(
            target=self._ttl_worker_loop,
            name="cluster-ttl-worker",
            daemon=True,
        )
        self._ttl_thread.start()

    def _stop_ttl_worker(self) -> None:
        """Stop background TTL eviction worker."""
        self._ttl_stop.set()
        if self._ttl_thread is not None:
            self._ttl_thread.join(timeout=1.0)
            self._ttl_thread = None

    def _ttl_worker_loop(self) -> None:
        """
        Periodically evict expired items using ordered mutation operations.
        """
        interval = float(self.config.collection_ttl.sweep_interval_seconds)
        while not self._ttl_stop.is_set():
            if not self.is_running:
                self._ttl_stop.wait(interval)
                continue
            if self.config.consensus.enabled and not self._is_leader():
                self._ttl_stop.wait(interval)
                continue

            due = self._collect_due_ttl_evictions(
                limit=int(self.config.collection_ttl.max_evictions_per_cycle)
            )
            if not due:
                self._ttl_stop.wait(interval)
                continue

            for operation in due:
                if self._ttl_stop.is_set() or not self.is_running:
                    return
                try:
                    self._submit_local_operation(
                        collection=str(operation["collection"]),
                        name=str(operation["name"]),
                        action=str(operation["action"]),
                        values=dict(operation["values"]),
                    )
                except Exception:
                    continue

    def _collect_due_ttl_evictions(self, *, limit: int) -> list[dict[str, Any]]:
        """
        Collect at most ``limit`` currently expired item-eviction operations.
        """
        now_ms = int(time.time() * 1000)
        planned: list[dict[str, Any]] = []
        if limit <= 0:
            return planned

        with self._ttl_lock:
            for name in sorted(self._ttl_map_entries):
                items = self._ttl_map_entries.get(name, {})
                for key, expires_at in sorted(items.items()):
                    if int(expires_at) > now_ms:
                        continue
                    planned.append(
                        {
                            "collection": "map",
                            "name": name,
                            "action": "expire_key",
                            "values": {
                                "key": key,
                                "expected_expires_at_ms": int(expires_at),
                                "eviction_reason": "ttl",
                            },
                        }
                    )
                    if len(planned) >= limit:
                        return planned

            for name in sorted(self._ttl_list_entries):
                items = self._ttl_list_entries.get(name, [])
                for index, entry in enumerate(items):
                    if not isinstance(entry, dict):
                        continue
                    expires_at = int(entry.get("expires_at_ms", 0))
                    if expires_at > now_ms:
                        continue
                    planned.append(
                        {
                            "collection": "list",
                            "name": name,
                            "action": "expire_index",
                            "values": {
                                "index": index,
                                "token": str(entry.get("token", "")),
                                "expected_expires_at_ms": expires_at,
                                "eviction_reason": "ttl",
                            },
                        }
                    )
                    if len(planned) >= limit:
                        return planned

            for name in sorted(self._ttl_queue_entries):
                items = list(self._ttl_queue_entries.get(name, deque()))
                for index, entry in enumerate(items):
                    if not isinstance(entry, dict):
                        continue
                    expires_at = int(entry.get("expires_at_ms", 0))
                    if expires_at > now_ms:
                        continue
                    planned.append(
                        {
                            "collection": "queue",
                            "name": name,
                            "action": "expire_index",
                            "values": {
                                "index": index,
                                "token": str(entry.get("token", "")),
                                "expected_expires_at_ms": expires_at,
                                "eviction_reason": "ttl",
                            },
                        }
                    )
                    if len(planned) >= limit:
                        return planned

        return planned

    def _ttl_snapshot_payload(self) -> dict[str, Any]:
        """Build serializable snapshot payload for TTL metadata."""
        with self._ttl_lock:
            list_entries = {
                name: [dict(item) if isinstance(item, dict) else None for item in entries]
                for name, entries in self._ttl_list_entries.items()
            }
            queue_entries = {
                name: [dict(item) if isinstance(item, dict) else None for item in list(entries)]
                for name, entries in self._ttl_queue_entries.items()
            }
            map_entries = {
                name: {key: int(expires_at) for key, expires_at in entries.items()}
                for name, entries in self._ttl_map_entries.items()
            }
            return {
                "component_ttl_seconds": {
                    "map": dict(self._map_ttl_seconds),
                    "list": dict(self._list_ttl_seconds),
                    "queue": dict(self._queue_ttl_seconds),
                },
                "map_entries": map_entries,
                "list_entries": list_entries,
                "queue_entries": queue_entries,
            }

    def _load_ttl_snapshot_payload(self, payload: dict[str, Any]) -> None:
        """Load TTL snapshot payload into runtime memory."""
        raw_component = payload.get("component_ttl_seconds", {})
        raw_map_ttl: dict[str, float] = {}
        raw_list_ttl: dict[str, float] = {}
        raw_queue_ttl: dict[str, float] = {}
        if isinstance(raw_component, dict):
            for collection_name, target in (
                ("map", raw_map_ttl),
                ("list", raw_list_ttl),
                ("queue", raw_queue_ttl),
            ):
                raw_mapping = raw_component.get(collection_name, {})
                if not isinstance(raw_mapping, dict):
                    continue
                for name, ttl_seconds in raw_mapping.items():
                    try:
                        normalized = float(ttl_seconds)
                    except (TypeError, ValueError):
                        continue
                    if normalized > 0:
                        target[str(name)] = normalized

        parsed_map_entries: dict[str, dict[str, int]] = {}
        raw_map_entries = payload.get("map_entries", {})
        if isinstance(raw_map_entries, dict):
            for name, raw_entries in raw_map_entries.items():
                if not isinstance(raw_entries, dict):
                    continue
                parsed_entries: dict[str, int] = {}
                for key, expires_at in raw_entries.items():
                    try:
                        parsed_entries[str(key)] = int(expires_at)
                    except (TypeError, ValueError):
                        continue
                parsed_map_entries[str(name)] = parsed_entries

        parsed_list_entries: dict[str, list[dict[str, Any] | None]] = {}
        raw_list_entries = payload.get("list_entries", {})
        if isinstance(raw_list_entries, dict):
            for name, raw_entries in raw_list_entries.items():
                if not isinstance(raw_entries, list):
                    continue
                parsed: list[dict[str, Any] | None] = []
                for raw in raw_entries:
                    if not isinstance(raw, dict):
                        parsed.append(None)
                        continue
                    token = str(raw.get("token", ""))
                    if not token:
                        parsed.append(None)
                        continue
                    try:
                        expires_at = int(raw.get("expires_at_ms", 0))
                    except (TypeError, ValueError):
                        parsed.append(None)
                        continue
                    parsed.append({"token": token, "expires_at_ms": expires_at})
                parsed_list_entries[str(name)] = parsed

        parsed_queue_entries: dict[str, deque[dict[str, Any] | None]] = {}
        raw_queue_entries = payload.get("queue_entries", {})
        if isinstance(raw_queue_entries, dict):
            for name, raw_entries in raw_queue_entries.items():
                if not isinstance(raw_entries, list):
                    continue
                parsed_items: list[dict[str, Any] | None] = []
                for raw in raw_entries:
                    if not isinstance(raw, dict):
                        parsed_items.append(None)
                        continue
                    token = str(raw.get("token", ""))
                    if not token:
                        parsed_items.append(None)
                        continue
                    try:
                        expires_at = int(raw.get("expires_at_ms", 0))
                    except (TypeError, ValueError):
                        parsed_items.append(None)
                        continue
                    parsed_items.append({"token": token, "expires_at_ms": expires_at})
                parsed_queue_entries[str(name)] = deque(parsed_items)

        with self._ttl_lock:
            self._map_ttl_seconds = raw_map_ttl
            self._list_ttl_seconds = raw_list_ttl
            self._queue_ttl_seconds = raw_queue_ttl
            self._ttl_map_entries = parsed_map_entries
            self._ttl_list_entries = parsed_list_entries
            self._ttl_queue_entries = parsed_queue_entries
            self._ttl_snapshot_loaded = True
