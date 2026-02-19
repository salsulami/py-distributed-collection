"""
Store protocol used by :class:`distributed_collections.cluster.ClusterNode`.

The cluster runtime depends on this abstract method surface rather than a
specific in-memory implementation, enabling optional external backends such as
Redis without changing distributed collection APIs.
"""

from __future__ import annotations

from typing import Any, Protocol


class CollectionStore(Protocol):
    """
    Behavioral contract for collection state backends.

    Implementations are expected to be safe for concurrent access, because node
    runtime threads can call read/write methods simultaneously.
    """

    is_centralized: bool
    """
    Indicates whether this backend is a centralized shared store.

    When true, all nodes read/write the same external state store directly.
    Cluster-level state replication for map/list/queue can therefore be skipped.
    """

    def apply_mutation(self, mutation: dict[str, Any]) -> Any:
        """Apply one collection mutation and return action-specific result."""

    def map_get(self, name: str, key: str, default: Any = None) -> Any:
        """Return map value or default when key is missing."""

    def map_items(self, name: str) -> dict[str, Any]:
        """Return full map snapshot."""

    def map_contains(self, name: str, key: str) -> bool:
        """Return true when map contains key."""

    def map_size(self, name: str) -> int:
        """Return map entry count."""

    def list_values(self, name: str) -> list[Any]:
        """Return list value snapshot."""

    def list_size(self, name: str) -> int:
        """Return list length."""

    def queue_values(self, name: str) -> list[Any]:
        """Return queue values from head to tail."""

    def queue_size(self, name: str) -> int:
        """Return queue length."""

    def queue_peek(self, name: str) -> Any:
        """Return queue head without removal."""

    def create_snapshot(self) -> dict[str, Any]:
        """Create serializable state snapshot for sync/persistence."""

    def load_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Replace backend state from snapshot."""
