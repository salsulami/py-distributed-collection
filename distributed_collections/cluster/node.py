"""
Cluster node runtime for distributed collection replication.

This module provides the concrete ``ClusterNode`` class while delegating domain
behavior to focused mixins.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from typing import Any

from ..config import ClusterConfig, NodeAddress
from ..discovery import MulticastDiscoveryResponder
from ..observability import ObservabilityServer
from ..persistence import SnapshotPersistence
from ..primitives import (
    DistributedList,
    DistributedLock,
    DistributedMap,
    DistributedQueue,
    DistributedTopic,
)
from ..store import ClusterDataStore
from ..store_protocol import CollectionStore
from ..transport import TcpTransportServer
from ..wal import WriteAheadLog
from .consensus import ClusterConsensusMixin
from .collection_events import ClusterCollectionEventsMixin
from .cp import ClusterCPMixin
from .diagnostics import ClusterDiagnosticsMixin
from .durability import ClusterDurabilityMixin
from .handles import ClusterHandleMixin
from .helpers import ClusterHelperMixin
from .lifecycle import ClusterLifecycleMixin
from .membership import ClusterMembershipMixin
from .observability import ClusterObservabilityMixin
from .operations import ClusterOperationMixin
from .primitive_adapters import ClusterPrimitiveAdapterMixin
from .replication import ClusterReplicationMixin
from .security import ClusterSecurityMixin
from .topics import ClusterTopicMixin
from .transport import ClusterTransportMixin
from .ttl import ClusterTTLMixin


class ClusterNode(
    ClusterLifecycleMixin,
    ClusterDiagnosticsMixin,
    ClusterMembershipMixin,
    ClusterHandleMixin,
    ClusterTransportMixin,
    ClusterCollectionEventsMixin,
    ClusterOperationMixin,
    ClusterReplicationMixin,
    ClusterDurabilityMixin,
    ClusterConsensusMixin,
    ClusterSecurityMixin,
    ClusterObservabilityMixin,
    ClusterHelperMixin,
    ClusterTTLMixin,
    ClusterCPMixin,
    ClusterTopicMixin,
    ClusterPrimitiveAdapterMixin,
):
    """
    Runtime container for distributed collections on one Python process.

    The node owns discovery, membership, transport, durability, and consistency
    orchestration for distributed map/list/queue/topic primitives.
    """

    @classmethod
    def from_backend(
        cls,
        config: ClusterConfig,
        *,
        backend: str = "memory",
        **backend_options: Any,
    ) -> "ClusterNode":
        """
        Build a node with a named store backend in one call.

        Examples
        --------
        In-memory backend (default)::

            node = ClusterNode.from_backend(config, backend="memory")

        Redis backend (optional plugin)::

            node = ClusterNode.from_backend(
                config,
                backend="redis",
                redis_url="redis://127.0.0.1:6379/0",
                namespace="orders",
            )
        """
        from ..backends import create_store

        return cls(config, store=create_store(backend=backend, **backend_options))

    def __init__(self, config: ClusterConfig, *, store: CollectionStore | None = None) -> None:
        """
        Initialize runtime state from :class:`ClusterConfig`.

        Parameters
        ----------
        config:
            Cluster runtime configuration.
        store:
            Optional external collection-store backend implementation. When not
            provided, the default in-memory :class:`ClusterDataStore` is used.
        """
        self.config = config
        self.node_id = uuid.uuid4().hex
        self._store: CollectionStore = store if store is not None else ClusterDataStore()
        self._store_is_centralized = bool(getattr(self._store, "is_centralized", False))

        # Membership state
        self._members: set[NodeAddress] = set()
        self._member_nodes: dict[str, NodeAddress] = {self.node_id: self.config.advertise_address}
        self._members_lock = threading.RLock()
        self._member_failures: dict[NodeAddress, int] = {}
        self._member_failures_lock = threading.RLock()

        # Lifecycle state
        self._lifecycle_lock = threading.RLock()
        self._running = False

        # Security transport contexts
        self._server_ssl_context = self._build_server_ssl_context()
        self._client_ssl_context = self._build_client_ssl_context()

        self._transport = TcpTransportServer(
            bind=self.config.bind,
            message_handler=self._handle_transport_message,
            ssl_context=self._server_ssl_context,
        )
        self._discovery_responder: MulticastDiscoveryResponder | None = None

        # Replication queue/worker state
        self._replication_queue: queue.Queue[
            tuple[NodeAddress, dict[str, Any], int] | None
        ] = queue.Queue(maxsize=self.config.replication.queue_maxsize)
        self._replication_stop = threading.Event()
        self._replication_threads: list[threading.Thread] = []

        # Persistence/WAL state
        self._persistence: SnapshotPersistence | None = None
        if self.config.persistence.enabled:
            self._persistence = SnapshotPersistence(
                self.config.persistence.snapshot_path,
                fsync=self.config.persistence.fsync,
            )
        self._wal: WriteAheadLog | None = None
        if self.config.wal.enabled:
            self._wal = WriteAheadLog(
                self.config.wal.wal_path,
                fsync_each_write=self.config.wal.fsync_each_write,
            )
        self._ops_since_checkpoint = 0

        self._persist_request = threading.Event()
        self._persist_stop = threading.Event()
        self._persist_thread: threading.Thread | None = None

        # Consensus/election state
        self._consensus_lock = threading.RLock()
        self._consensus_stop = threading.Event()
        self._consensus_thread: threading.Thread | None = None
        self._term = 0
        self._voted_for: str | None = None
        self._leader_id: str | None = None
        self._leader_address: NodeAddress | None = None
        now = time.monotonic()
        self._last_heartbeat_received = now
        self._last_majority_contact = 0.0
        self._next_log_index = 1
        self._last_applied_index = 0
        self._pending_ordered_ops: dict[int, dict[str, Any]] = {}

        # Operation deduplication state
        self._seen_operation_ids: set[str] = set()
        self._seen_operation_order: deque[str] = deque()
        self._seen_operation_limit = 100_000
        self._seen_lock = threading.Lock()

        # CP lock state (replicated via ordered operation log)
        self._cp_state_lock = threading.RLock()
        self._cp_locks: dict[str, dict[str, Any]] = {}
        self._cp_lock_fencing_counters: dict[str, int] = {}

        # Topic subscriber state
        self._topic_subscribers: dict[str, dict[str, Callable[[Any], None]]] = {}
        self._topic_lock = threading.RLock()

        # Collection item listener state
        self._collection_listener_lock = threading.RLock()
        self._collection_listeners: dict[
            tuple[str, str], dict[str, Callable[[dict[str, Any]], None]]
        ] = {}

        # Component-level TTL config/runtime state
        self._ttl_lock = threading.RLock()
        self._map_ttl_seconds: dict[str, float] = dict(self.config.collection_ttl.map_ttl_seconds)
        self._list_ttl_seconds: dict[str, float] = dict(self.config.collection_ttl.list_ttl_seconds)
        self._queue_ttl_seconds: dict[str, float] = dict(self.config.collection_ttl.queue_ttl_seconds)
        self._ttl_map_entries: dict[str, dict[str, int]] = {}
        self._ttl_list_entries: dict[str, list[dict[str, Any] | None]] = {}
        self._ttl_queue_entries: dict[str, deque[dict[str, Any] | None]] = {}
        self._ttl_snapshot_loaded = False
        self._ttl_stop = threading.Event()
        self._ttl_thread: threading.Thread | None = None

        # Public handle caches
        self._map_handles: dict[str, DistributedMap] = {}
        self._list_handles: dict[str, DistributedList] = {}
        self._queue_handles: dict[str, DistributedQueue] = {}
        self._topic_handles: dict[str, DistributedTopic] = {}
        self._lock_handles: dict[str, DistributedLock] = {}
        self._handle_lock = threading.RLock()

        # Observability/tracing state
        self._observability_server: ObservabilityServer | None = None
        self._trace_lock = threading.Lock()
        self._trace_history: deque[dict[str, Any]] = deque(
            maxlen=self.config.observability.trace_history_size
        )
        self._stats_lock = threading.Lock()
        self._stats: dict[str, int] = {
            "local_mutations": 0,
            "remote_mutations": 0,
            "forwarded_writes": 0,
            "leader_elections_started": 0,
            "leader_elections_won": 0,
            "leader_heartbeats_sent": 0,
            "leader_heartbeats_acked": 0,
            "replication_enqueued": 0,
            "replication_success": 0,
            "replication_failures": 0,
            "replication_backpressure_drops": 0,
            "snapshot_load_success": 0,
            "snapshot_load_failures": 0,
            "snapshot_save_success": 0,
            "snapshot_save_failures": 0,
            "wal_append_success": 0,
            "wal_append_failures": 0,
            "wal_replay_success": 0,
            "wal_replay_failures": 0,
            "auth_failures": 0,
            "protocol_failures": 0,
            "acl_denied": 0,
            "dropped_messages": 0,
            "members_evicted": 0,
            "quorum_write_failures": 0,
            "cp_operations": 0,
            "cp_lock_acquire_success": 0,
            "cp_lock_acquire_conflicts": 0,
            "cp_lock_release_success": 0,
            "cp_lock_release_failures": 0,
            "cp_lock_refresh_success": 0,
            "cp_lock_refresh_failures": 0,
            "cp_lock_expired": 0,
        }
