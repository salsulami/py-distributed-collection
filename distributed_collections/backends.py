"""
Backend factory helpers for easy store switching.

This module gives application developers a uniform way to pick a storage
backend by name without rewriting node bootstrap logic.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from .exceptions import BackendConfigurationError, BackendNotAvailableError
from .store import ClusterDataStore
from .store_protocol import CollectionStore

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from .cluster import ClusterNode
    from .config import ClusterConfig


class StoreBackend(str, Enum):
    """
    Built-in backend names supported by the factory helpers.

    MEMORY
        In-process in-memory store.
    REDIS
        Centralized Redis store provided by optional plugin package.
    """

    MEMORY = "memory"
    REDIS = "redis"


def _normalize_backend(backend: str | StoreBackend) -> StoreBackend:
    """
    Normalize backend name into :class:`StoreBackend` enum value.
    """
    if isinstance(backend, StoreBackend):
        return backend
    lowered = str(backend).strip().lower()
    try:
        return StoreBackend(lowered)
    except ValueError as exc:
        valid = ", ".join(item.value for item in StoreBackend)
        raise BackendConfigurationError(
            f"Unknown backend {backend!r}. Supported values: {valid}."
        ) from exc


def available_backends() -> tuple[str, ...]:
    """
    Return backend names available in the current environment.

    The Redis backend appears only when the optional plugin package is installed.
    """
    backends = [StoreBackend.MEMORY.value]
    try:
        __import__("distributed_collections_redis")
    except Exception:  # noqa: BLE001 - optional dependency probing
        pass
    else:
        backends.append(StoreBackend.REDIS.value)
    return tuple(backends)


def create_store(backend: str | StoreBackend = StoreBackend.MEMORY, **backend_options: Any) -> CollectionStore:
    """
    Create a store backend instance from a short backend name.

    Parameters
    ----------
    backend:
        Backend selector string (``"memory"`` or ``"redis"``).
    backend_options:
        Backend-specific options.

        Redis options:
            ``redis_url`` (str), ``namespace`` (str), ``redis_client``
            and optional plugin-native ``config`` object.
    """
    selected = _normalize_backend(backend)
    if selected is StoreBackend.MEMORY:
        if backend_options:
            unknown = ", ".join(sorted(str(key) for key in backend_options))
            raise BackendConfigurationError(
                f"Memory backend does not accept options: {unknown}."
            )
        return ClusterDataStore()
    if selected is StoreBackend.REDIS:
        try:
            from distributed_collections_redis import RedisClusterDataStore, RedisStoreConfig
        except Exception as exc:  # noqa: BLE001 - optional dependency may be absent
            raise BackendNotAvailableError(
                "Redis backend requires optional package "
                "'distributed-python-collections-redis'."
            ) from exc

        config = backend_options.pop("config", None)
        redis_client = backend_options.pop("redis_client", None)
        if config is None:
            redis_url = str(
                backend_options.pop("redis_url", "redis://127.0.0.1:6379/0")
            )
            namespace = str(backend_options.pop("namespace", "distributed-collections"))
            config = RedisStoreConfig(redis_url=redis_url, namespace=namespace)
        if backend_options:
            unknown = ", ".join(sorted(str(key) for key in backend_options))
            raise BackendConfigurationError(
                f"Unknown Redis backend options: {unknown}."
            )
        return RedisClusterDataStore(config=config, redis_client=redis_client)
    raise BackendConfigurationError(f"Unhandled backend: {selected!r}")


def create_node(
    config: "ClusterConfig",
    *,
    backend: str | StoreBackend = StoreBackend.MEMORY,
    **backend_options: Any,
) -> "ClusterNode":
    """
    Build :class:`ClusterNode` using named backend in one step.

    This helper avoids explicit store wiring code:

    ``node = create_node(config, backend="redis", redis_url="...")``
    """
    from .cluster import ClusterNode

    store = create_store(backend=backend, **backend_options)
    return ClusterNode(config, store=store)
