"""
Cluster node runtime for distributed collection replication.

This module contains :class:`ClusterNode`, a production-focused runtime that
adds:

* leader election with heartbeat-based failure detection
* quorum-aware write consistency and ordered replication
* WAL + snapshot durability lifecycle
* authenticated/TLS transport support
* batching, backpressure, and flow control for replication
* health, metrics, and tracing surfaces
"""

from __future__ import annotations

import queue
import random
import ssl
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from typing import Any

from .config import (
    ClusterConfig,
    ConsistencyMode,
    DiscoveryMode,
    NodeAddress,
)
from .discovery import (
    MulticastDiscoveryResponder,
    discover_multicast_peers,
    discover_static_peers,
    merge_discovery_results,
)
from .exceptions import (
    AuthenticationError,
    ClusterNotRunningError,
    ProtocolVersionError,
)
from .observability import ObservabilityServer
from .persistence import SnapshotPersistence
from .primitives import (
    DistributedList,
    DistributedLock,
    DistributedMap,
    DistributedQueue,
    DistributedTopic,
)
from .protocol import (
    MessageKind,
    assert_authenticated,
    assert_protocol_compatible,
    make_message,
)
from .store import ClusterDataStore
from .store_protocol import CollectionStore
from .transport import TcpTransportServer, request_response
from .wal import WriteAheadLog

_REPLICATION_SENTINEL: tuple[NodeAddress, dict[str, Any], int] | None = None


class ClusterNode:
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
        from .backends import create_store

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

    # --------------------------------------------------------------------- #
    # Lifecycle
    # --------------------------------------------------------------------- #

    def start(self, *, join: bool = True) -> None:
        """
        Start transport, workers, optional observability, and discovery.
        """
        with self._lifecycle_lock:
            if self._running:
                return

            if not self._store_is_centralized:
                self._load_snapshot_if_enabled()
                self._replay_wal_if_enabled()

            self._transport.start()
            self._start_replication_workers()
            if not self._store_is_centralized:
                self._start_persistence_worker()
            self._start_consensus_worker()
            self._start_observability_server()

            if DiscoveryMode.MULTICAST in self.config.enabled_discovery:
                self._discovery_responder = MulticastDiscoveryResponder(
                    cluster_name=self.config.cluster_name,
                    node_id=self.node_id,
                    advertise=self.config.advertise_address,
                    group=self.config.multicast.group,
                    port=self.config.multicast.port,
                    socket_timeout_seconds=self.config.socket_timeout_seconds,
                )
                self._discovery_responder.start()

            self._running = True
            if self.config.consensus.enabled:
                with self._consensus_lock:
                    if self._leader_id is None and not self.members():
                        self._term = max(self._term, 1)
                        self._leader_id = self.node_id
                        self._leader_address = self.config.advertise_address
                        now = time.monotonic()
                        self._last_heartbeat_received = now
                        self._last_majority_contact = now
            self._trace("node_started")

        if join:
            self.join_cluster()

    def stop(self) -> None:
        """
        Stop listeners/workers and flush final durable state.
        """
        with self._lifecycle_lock:
            if not self._running:
                return
            self._running = False

            if self._discovery_responder:
                self._discovery_responder.stop()
                self._discovery_responder = None

            self._stop_consensus_worker()
            self._stop_replication_workers()
            if not self._store_is_centralized:
                self._stop_persistence_worker(flush=True)
            self._stop_observability_server()
            self._transport.stop()
            self._trace("node_stopped")

    def close(self) -> None:
        """Alias for :meth:`stop`."""
        self.stop()

    @property
    def is_running(self) -> bool:
        """Return whether the node currently accepts runtime traffic."""
        with self._lifecycle_lock:
            return self._running

    # --------------------------------------------------------------------- #
    # Public diagnostics API
    # --------------------------------------------------------------------- #

    def stats(self) -> dict[str, Any]:
        """
        Return cumulative runtime counters and live gauges.
        """
        with self._stats_lock:
            payload = dict(self._stats)
        payload["member_count"] = len(self.members())
        payload["replication_queue_depth"] = self._replication_queue.qsize()
        payload["running"] = self.is_running
        payload["leader"] = self._leader_id
        payload["is_leader"] = self._is_leader()
        payload["term"] = self._term
        payload["last_applied_index"] = self._last_applied_index
        payload["cp_lock_count"] = self._cp_lock_count()
        return payload

    def health(self) -> dict[str, Any]:
        """
        Return structured health state for readiness/liveness checks.
        """
        status = "ok"
        if not self.is_running:
            status = "stopped"
        elif self._is_leader() and self.config.consensus.require_majority_for_writes:
            if not self._leader_lease_valid():
                status = "degraded"
        return {
            "status": status,
            "cluster": self.config.cluster_name,
            "node_id": self.node_id,
            "leader_id": self._leader_id,
            "is_leader": self._is_leader(),
            "member_count": len(self.members()),
            "term": self._term,
            "last_applied_index": self._last_applied_index,
            "protocol_min": self.config.upgrade.min_compatible_protocol_version,
            "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            "protocol_local": self.config.upgrade.max_compatible_protocol_version,
        }

    def metrics_text(self) -> str:
        """
        Return Prometheus-style metrics payload as text.
        """
        stats = self.stats()
        lines = []
        for key, value in sorted(stats.items()):
            if isinstance(value, bool):
                numeric = 1 if value else 0
                lines.append(f"distributed_collections_{key} {numeric}")
            elif isinstance(value, (int, float)):
                lines.append(f"distributed_collections_{key} {value}")
        return "\n".join(lines) + "\n"

    def recent_traces(self) -> dict[str, Any]:
        """
        Return bounded in-memory trace event history.
        """
        with self._trace_lock:
            traces = list(self._trace_history)
        return {"traces": traces}

    # --------------------------------------------------------------------- #
    # Discovery and membership
    # --------------------------------------------------------------------- #

    def join_cluster(self) -> list[NodeAddress]:
        """
        Discover peers, perform handshakes, and optionally synchronize state.
        """
        self._ensure_running()

        discovered_groups: list[list[NodeAddress]] = []
        if DiscoveryMode.STATIC in self.config.enabled_discovery:
            discovered_groups.append(discover_static_peers(self.config))
        if DiscoveryMode.MULTICAST in self.config.enabled_discovery:
            discovered_groups.append(
                discover_multicast_peers(
                    config=self.config,
                    node_id=self.node_id,
                    socket_timeout_seconds=self.config.socket_timeout_seconds,
                )
            )
        discovered = merge_discovery_results(discovered_groups)
        reachable_peers: list[NodeAddress] = []

        for peer in discovered:
            payload = self._handshake_peer(peer)
            if payload is None:
                continue
            reachable_peers.append(peer)

            remote_node_id = payload.get("node_id")
            if isinstance(remote_node_id, str):
                self._add_member(peer, node_id=remote_node_id)
            else:
                self._add_member(peer)

            for member_node_id, member_address in self._member_nodes_from_payload(
                payload.get("members", [])
            ):
                self._add_member(member_address, node_id=member_node_id)

        if self.config.auto_sync_on_join and reachable_peers and not self._store_is_centralized:
            self._sync_from_peer_list(reachable_peers)
        self._trace("join_completed", discovered=len(discovered), reachable=len(reachable_peers))
        return discovered

    def members(self) -> list[NodeAddress]:
        """
        Return known remote member addresses sorted by host/port.
        """
        with self._members_lock:
            return sorted(self._members, key=lambda item: (item.host, item.port))

    def wait_for_next_join_window(self) -> None:
        """Sleep for configured reconnect interval."""
        time.sleep(float(self.config.reconnect_interval_seconds))

    # --------------------------------------------------------------------- #
    # Public distributed collection accessors
    # --------------------------------------------------------------------- #

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

    # --------------------------------------------------------------------- #
    # Transport message handling
    # --------------------------------------------------------------------- #

    def _handle_transport_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        Process one inbound protocol message and return one response envelope.
        """
        try:
            assert_protocol_compatible(
                message,
                min_supported_version=self.config.upgrade.min_compatible_protocol_version,
                max_supported_version=self.config.upgrade.max_compatible_protocol_version,
            )
        except ProtocolVersionError as exc:
            self._inc_stat("protocol_failures")
            self._inc_stat("dropped_messages")
            return self._make_message(MessageKind.ERROR, {"reason": str(exc)})

        try:
            assert_authenticated(message, self.config.security.shared_token)
        except AuthenticationError as exc:
            self._inc_stat("auth_failures")
            self._inc_stat("dropped_messages")
            return self._make_message(MessageKind.ERROR, {"reason": str(exc)})

        if message.get("cluster") != self.config.cluster_name:
            self._inc_stat("dropped_messages")
            return self._make_message(MessageKind.ERROR, {"reason": "cluster mismatch"})

        kind = str(message.get("kind", ""))
        sender_node_id = str(message.get("sender_node_id", ""))
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}

        if kind == MessageKind.HANDSHAKE.value:
            return self._handle_handshake(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.STATE_REQUEST.value:
            return self._make_message(
                MessageKind.STATE_RESPONSE,
                {"snapshot": self._snapshot_payload()},
            )
        if kind == MessageKind.OPERATION.value:
            try:
                self._handle_remote_operation(payload, sender_node_id=sender_node_id)
            except (PermissionError, ValueError) as exc:
                return self._make_message(MessageKind.ERROR, {"reason": str(exc)})
            return self._make_message(MessageKind.OPERATION, {"status": "ok"})
        if kind == MessageKind.OPERATION_BATCH.value:
            operations = payload.get("operations", [])
            if isinstance(operations, list):
                try:
                    for item in operations:
                        if isinstance(item, dict):
                            self._handle_remote_operation(item, sender_node_id=sender_node_id)
                except (PermissionError, ValueError) as exc:
                    return self._make_message(MessageKind.ERROR, {"reason": str(exc)})
            return self._make_message(MessageKind.OPERATION_BATCH, {"status": "ok"})
        if kind == MessageKind.FORWARD_OPERATION.value:
            return self._handle_forwarded_operation(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.VOTE_REQUEST.value:
            return self._handle_vote_request(payload, sender_node_id=sender_node_id)
        if kind == MessageKind.HEARTBEAT.value:
            return self._handle_heartbeat(payload, sender_node_id=sender_node_id)
        return self._make_message(
            MessageKind.ERROR,
            {"reason": f"unsupported message kind: {kind!r}"},
        )

    def _handle_handshake(self, payload: dict[str, Any], *, sender_node_id: str) -> dict[str, Any]:
        """
        Register handshake peer and return known cluster members.
        """
        peer_min = int(payload.get("protocol_min", 1))
        peer_max = int(payload.get("protocol_max", 1))
        if not self._protocol_ranges_overlap(peer_min, peer_max):
            return self._make_message(
                MessageKind.ERROR,
                {"reason": "protocol compatibility range mismatch"},
            )

        raw_address = payload.get("address")
        if isinstance(raw_address, dict):
            try:
                address = NodeAddress.from_dict(raw_address)
                node_id = str(payload.get("node_id", "")) or sender_node_id or None
                self._add_member(address, node_id=node_id)
            except (KeyError, TypeError, ValueError):
                pass

        return self._make_message(
            MessageKind.HANDSHAKE_ACK,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "members": self._member_nodes_payload(),
                "protocol_min": self.config.upgrade.min_compatible_protocol_version,
                "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            },
        )

    def _handle_forwarded_operation(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Execute a follower-forwarded write operation on leader.
        """
        if self.config.consensus.enabled and not self._is_leader():
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {
                    "status": "error",
                    "reason": "not leader",
                    "leader_address": (
                        self._leader_address.as_dict() if self._leader_address is not None else None
                    ),
                },
            )
        if (
            self.config.consensus.enabled
            and self.config.consensus.require_majority_for_writes
            and not self._leader_lease_valid()
        ):
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "leader quorum lease expired"},
            )
        operation = payload.get("operation")
        if not isinstance(operation, dict):
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "missing operation payload"},
            )
        if not self._is_acl_allowed(sender_node_id=sender_node_id, payload=operation):
            self._inc_stat("acl_denied")
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": "ACL denied for forwarded operation"},
            )
        try:
            result = self._execute_leader_operation(operation, origin_node_id=sender_node_id or self.node_id)
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "ok", "result": result},
            )
        except Exception as exc:  # noqa: BLE001 - forwarded operation returns structured errors
            return self._make_message(
                MessageKind.FORWARD_OPERATION_RESULT,
                {"status": "error", "reason": str(exc)},
            )

    def _handle_vote_request(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Process one leader-election vote request.
        """
        candidate_id = str(payload.get("candidate_id", ""))
        request_term = int(payload.get("term", 0))
        candidate_last_index = int(payload.get("last_log_index", 0))
        granted = False

        if sender_node_id and sender_node_id == candidate_id:
            raw = payload.get("address")
            if isinstance(raw, dict):
                try:
                    self._add_member(NodeAddress.from_dict(raw), node_id=candidate_id)
                except (TypeError, ValueError, KeyError):
                    pass

        with self._consensus_lock:
            if request_term < self._term:
                granted = False
            else:
                if request_term > self._term:
                    self._term = request_term
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_address = None
                up_to_date = candidate_last_index >= self._last_applied_index
                if up_to_date and (self._voted_for is None or self._voted_for == candidate_id):
                    self._voted_for = candidate_id
                    self._last_heartbeat_received = time.monotonic()
                    granted = True

        return self._make_message(
            MessageKind.VOTE_RESPONSE,
            {
                "term": self._term,
                "granted": granted,
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )

    def _handle_heartbeat(
        self,
        payload: dict[str, Any],
        *,
        sender_node_id: str,
    ) -> dict[str, Any]:
        """
        Process one leader heartbeat and refresh lease/failure-detector state.
        """
        leader_term = int(payload.get("term", 0))
        leader_id = str(payload.get("leader_id", sender_node_id or ""))
        raw_address = payload.get("address")
        leader_address: NodeAddress | None = None
        if isinstance(raw_address, dict):
            try:
                leader_address = NodeAddress.from_dict(raw_address)
            except (TypeError, ValueError, KeyError):
                leader_address = None

        leader_changed = False
        with self._consensus_lock:
            if leader_term >= self._term:
                if leader_term > self._term:
                    self._term = leader_term
                    self._voted_for = None
                if leader_id:
                    leader_changed = leader_id != self._leader_id
                    self._leader_id = leader_id
                if leader_address is not None:
                    self._leader_address = leader_address
                self._last_heartbeat_received = time.monotonic()

        if leader_id and leader_address is not None:
            self._add_member(leader_address, node_id=leader_id)

        # Partition-healing bootstrap: on leader change, re-sync state snapshot.
        if leader_changed and leader_address is not None and self.config.auto_sync_on_join:
            threading.Thread(
                target=self._sync_from_peer_list,
                args=([leader_address],),
                name="cluster-resync-on-leader-change",
                daemon=True,
            ).start()

        return self._make_message(
            MessageKind.HEARTBEAT_ACK,
            {
                "term": self._term,
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )

    # --------------------------------------------------------------------- #
    # Operation application and consistency
    # --------------------------------------------------------------------- #

    def _submit_local_operation(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> Any:
        """
        Submit one local mutation under configured consistency policy.
        """
        self._ensure_running()

        operation: dict[str, Any] = {
            "collection": collection,
            "name": name,
            "action": action,
        }
        if values:
            operation.update(values)

        if self._is_cp_operation(collection) and not self.config.cp.enabled:
            raise ClusterNotRunningError("CP subsystem is disabled.")

        mode = self._effective_consistency_mode(collection)
        if self.config.consensus.enabled and mode != ConsistencyMode.BEST_EFFORT:
            if not self._is_leader():
                return self._forward_operation_to_leader(operation)
            if self.config.consensus.require_majority_for_writes and not self._leader_lease_valid():
                self._inc_stat("quorum_write_failures")
                raise ClusterNotRunningError(
                    "Leader quorum lease expired; refusing write to avoid split-brain divergence."
                )

        return self._execute_leader_operation(operation, origin_node_id=self.node_id)

    def _execute_leader_operation(
        self,
        operation: dict[str, Any],
        *,
        origin_node_id: str,
    ) -> Any:
        """
        Execute one operation on leader and replicate using configured mode.
        """
        payload = self._build_operation_payload(operation, origin_node_id=origin_node_id)
        op_id = str(payload["op_id"])
        if not self._record_operation(op_id):
            return None

        collection_name = str(payload.get("collection", ""))
        self._append_wal_entry(payload)
        result = self._apply_payload(payload, source="local")
        self._inc_stat("local_mutations")
        self._schedule_persist()

        mode = self._effective_consistency_mode(collection_name)
        if self._is_centralized_data_operation(collection_name):
            return result

        # Commit strategy by consistency mode.
        required = self._required_ack_count(mode)
        if mode == ConsistencyMode.BEST_EFFORT:
            self._enqueue_replication(payload)
            return result

        acks = 1 + self._replicate_operation_sync(
            payload,
            required_acks=required,
            timeout_seconds=self.config.consistency.write_timeout_seconds,
        )
        self._enqueue_replication(payload)
        if acks < required:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError(
                f"Write commit failed; acknowledgements {acks} below required {required}."
            )
        return result

    def _forward_operation_to_leader(self, operation: dict[str, Any]) -> Any:
        """
        Forward write request to active leader for linearizable commit.
        """
        leader = self._resolve_leader_address()
        if leader is None and self.config.consensus.enabled:
            self._start_election()
            leader = self._resolve_leader_address()
            if leader == self.config.advertise_address:
                return self._execute_leader_operation(operation, origin_node_id=self.node_id)
        if leader is None:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("No known leader to forward operation.")

        request = self._make_message(
            MessageKind.FORWARD_OPERATION,
            {"operation": operation},
        )
        response = request_response(
            leader,
            request,
            self.config.consistency.write_timeout_seconds,
            security_token=self.config.security.shared_token,
            ssl_context=self._client_ssl_context,
            server_hostname=self.config.tls.server_hostname,
            min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
            max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
        )
        if response.get("kind") != MessageKind.FORWARD_OPERATION_RESULT.value:
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("Leader returned unexpected forwarded-write response.")
        payload = response.get("payload", {})
        if not isinstance(payload, dict):
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError("Leader returned malformed forwarded-write payload.")
        if payload.get("status") != "ok":
            self._inc_stat("quorum_write_failures")
            raise ClusterNotRunningError(str(payload.get("reason", "forwarded write rejected")))
        self._inc_stat("forwarded_writes")
        return payload.get("result")

    def _handle_remote_operation(self, payload: dict[str, Any], *, sender_node_id: str) -> None:
        """
        Validate, authorize, and apply one replicated operation payload.
        """
        op_id = str(payload.get("op_id", ""))
        if not op_id:
            self._inc_stat("dropped_messages")
            raise ValueError("operation payload missing op_id")
        if not self._record_operation(op_id):
            return

        if not self._is_acl_allowed(sender_node_id=sender_node_id, payload=payload):
            self._inc_stat("acl_denied")
            self._inc_stat("dropped_messages")
            raise PermissionError("ACL denied for sender operation")

        collection = str(payload.get("collection", ""))
        if self._is_centralized_data_operation(collection):
            # Centralized backends (for example Redis) are globally shared, so
            # replicated map/list/queue payloads do not need local re-apply.
            return
        if collection != "topic":
            self._append_wal_entry(payload)

        if self.config.consensus.enabled:
            self._apply_ordered_remote_payload(payload, sender_node_id=sender_node_id)
        else:
            self._apply_payload(payload, source="remote")
            self._inc_stat("remote_mutations")
            self._schedule_persist()

    def _apply_ordered_remote_payload(self, payload: dict[str, Any], *, sender_node_id: str) -> None:
        """
        Apply remote payload in leader-defined log order.
        """
        term = int(payload.get("term", 0))
        log_index = int(payload.get("log_index", 0))
        if log_index <= 0:
            self._apply_payload(payload, source="remote")
            self._inc_stat("remote_mutations")
            self._schedule_persist()
            return

        with self._consensus_lock:
            if term >= self._term:
                if term > self._term:
                    self._term = term
                    self._voted_for = None
                if sender_node_id:
                    self._leader_id = sender_node_id
                self._last_heartbeat_received = time.monotonic()
            if log_index <= self._last_applied_index:
                return
            self._pending_ordered_ops[log_index] = payload

            while True:
                next_index = self._last_applied_index + 1
                next_payload = self._pending_ordered_ops.pop(next_index, None)
                if next_payload is None:
                    break
                self._apply_payload(next_payload, source="remote")
                self._last_applied_index = next_index
                self._inc_stat("remote_mutations")
                self._schedule_persist()

    def _apply_payload(self, payload: dict[str, Any], *, source: str) -> Any:
        """
        Apply one payload to local in-memory state.
        """
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        name = str(payload.get("name", ""))
        trace_id = str(payload.get("trace_id", ""))

        if collection == "topic":
            if action == "publish":
                self._dispatch_topic(name, payload.get("message"))
                self._trace("topic_publish_applied", trace_id=trace_id, collection=name, source=source)
            return None

        if collection == "cp_lock":
            result = self._apply_cp_lock_payload(payload)
            self._inc_stat("cp_operations")
            self._trace(
                "cp_lock_operation_applied",
                trace_id=trace_id,
                collection=collection,
                name=name,
                action=action,
                source=source,
                log_index=int(payload.get("log_index", 0)),
            )
            return result

        result = self._store.apply_mutation(payload)
        log_index = int(payload.get("log_index", 0))
        if source == "local":
            with self._consensus_lock:
                if log_index > self._last_applied_index:
                    self._last_applied_index = log_index
        self._trace(
            "collection_mutation_applied",
            trace_id=trace_id,
            collection=collection,
            name=name,
            action=action,
            source=source,
            log_index=log_index,
        )
        return result

    def _build_operation_payload(self, operation: dict[str, Any], *, origin_node_id: str) -> dict[str, Any]:
        """
        Build ordered operation payload with term/index/trace metadata.
        """
        with self._consensus_lock:
            if self._term <= 0:
                self._term = 1
            term = self._term
            log_index = self._next_log_index
            self._next_log_index += 1
        trace_id = uuid.uuid4().hex
        payload: dict[str, Any] = {
            "op_id": uuid.uuid4().hex,
            "origin_node_id": origin_node_id,
            "collection": str(operation["collection"]),
            "name": str(operation["name"]),
            "action": str(operation["action"]),
            "term": term,
            "log_index": log_index,
            "trace_id": trace_id,
            "ts_ms": int(time.time() * 1000),
        }
        for key, value in operation.items():
            if key in {"collection", "name", "action"}:
                continue
            payload[key] = value
        return payload

    def _required_ack_count(self, mode: ConsistencyMode) -> int:
        """
        Compute required acknowledgements for selected consistency mode.
        """
        cluster_size = len(self.members()) + 1
        if mode == ConsistencyMode.BEST_EFFORT:
            return 1
        if mode in {ConsistencyMode.QUORUM, ConsistencyMode.LINEARIZABLE}:
            return (cluster_size // 2) + 1
        if mode == ConsistencyMode.ALL:
            return cluster_size
        return 1

    def _replicate_operation_sync(
        self,
        payload: dict[str, Any],
        *,
        required_acks: int,
        timeout_seconds: float,
    ) -> int:
        """
        Replicate one payload synchronously and return successful peer acks.
        """
        peers = self.members()
        if not peers:
            return 0
        successes = 0
        deadline = time.monotonic() + timeout_seconds
        for peer in peers:
            if 1 + successes >= required_acks:
                break
            if time.monotonic() >= deadline:
                break
            remaining = max(0.1, deadline - time.monotonic())
            if self._deliver_single_operation(peer, payload, timeout_seconds=remaining):
                successes += 1
        return successes

    # --------------------------------------------------------------------- #
    # Replication queue, batching, backpressure, and flow-control
    # --------------------------------------------------------------------- #

    def _start_replication_workers(self) -> None:
        """Start asynchronous replication worker threads."""
        self._replication_stop.clear()
        self._replication_threads.clear()
        for index in range(self.config.replication.worker_threads):
            thread = threading.Thread(
                target=self._replication_worker_loop,
                name=f"cluster-replication-{index}",
                daemon=True,
            )
            thread.start()
            self._replication_threads.append(thread)

    def _stop_replication_workers(self) -> None:
        """Stop replication worker threads."""
        self._replication_stop.set()
        for _ in self._replication_threads:
            self._replication_queue.put(_REPLICATION_SENTINEL)
        for thread in self._replication_threads:
            thread.join(timeout=1.0)
        self._replication_threads.clear()

    def _enqueue_replication(self, payload: dict[str, Any]) -> None:
        """
        Enqueue replication tasks with backpressure handling.
        """
        for peer in self.members():
            task = (peer, payload, 1)
            try:
                self._replication_queue.put(
                    task,
                    timeout=self.config.replication.enqueue_timeout_seconds,
                )
                self._inc_stat("replication_enqueued")
            except queue.Full:
                if self.config.replication.drop_on_overflow:
                    self._inc_stat("replication_backpressure_drops")
                    self._inc_stat("dropped_messages")
                    continue
                raise ClusterNotRunningError(
                    "Replication queue overflow; refusing operation to apply flow control."
                ) from None

    def _replication_worker_loop(self) -> None:
        """
        Replication worker: drains queue, batches by peer, and retries failures.
        """
        while not self._replication_stop.is_set():
            try:
                first = self._replication_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if first is _REPLICATION_SENTINEL:
                return

            batch: list[tuple[NodeAddress, dict[str, Any], int]] = [first]
            while len(batch) < self.config.replication.batch_size:
                try:
                    nxt = self._replication_queue.get_nowait()
                except queue.Empty:
                    break
                if nxt is _REPLICATION_SENTINEL:
                    self._replication_queue.put(_REPLICATION_SENTINEL)
                    break
                batch.append(nxt)

            grouped: dict[NodeAddress, list[tuple[dict[str, Any], int]]] = {}
            for peer, payload, attempt in batch:
                grouped.setdefault(peer, []).append((payload, attempt))

            for peer, items in grouped.items():
                payloads = [payload for payload, _ in items]
                ok = self._deliver_peer_batch(peer, payloads)
                if ok:
                    self._clear_member_failure(peer)
                    self._inc_stat("replication_success", delta=len(items))
                    continue

                self._inc_stat("replication_failures", delta=len(items))
                self._record_member_failure(peer)
                for payload, attempt in items:
                    if attempt >= self.config.replication.max_retries:
                        continue
                    if self._replication_stop.is_set():
                        continue
                    delay = min(
                        self.config.replication.max_backoff_seconds,
                        self.config.replication.initial_backoff_seconds * (2 ** (attempt - 1)),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    try:
                        self._replication_queue.put(
                            (peer, payload, attempt + 1),
                            timeout=self.config.replication.enqueue_timeout_seconds,
                        )
                    except queue.Full:
                        self._inc_stat("replication_backpressure_drops")

    def _deliver_peer_batch(self, peer: NodeAddress, payloads: list[dict[str, Any]]) -> bool:
        """
        Deliver one or many operation payloads to a peer in one request.
        """
        if not payloads:
            return True
        if len(payloads) == 1:
            return self._deliver_single_operation(
                peer,
                payloads[0],
                timeout_seconds=self.config.socket_timeout_seconds,
            )
        try:
            response = request_response(
                peer,
                self._make_message(MessageKind.OPERATION_BATCH, {"operations": payloads}),
                self.config.socket_timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
            if response.get("kind") == MessageKind.ERROR.value:
                return False
            return True
        except Exception:
            return False

    def _deliver_single_operation(
        self,
        peer: NodeAddress,
        payload: dict[str, Any],
        *,
        timeout_seconds: float,
    ) -> bool:
        """
        Deliver one operation to one peer and return success status.
        """
        try:
            response = request_response(
                peer,
                self._make_message(MessageKind.OPERATION, payload),
                timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
            if response.get("kind") == MessageKind.ERROR.value:
                return False
            return True
        except Exception:
            return False

    # --------------------------------------------------------------------- #
    # Durability lifecycle: snapshot + WAL
    # --------------------------------------------------------------------- #

    def _load_snapshot_if_enabled(self) -> None:
        """
        Load persisted snapshot state and metadata if configured.
        """
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        try:
            payload = self._persistence.load()
            if payload is None:
                return
            state: dict[str, Any]
            metadata: dict[str, Any]
            has_cp_payload = False
            if "state" in payload and isinstance(payload.get("state"), dict):
                state = dict(payload["state"])
                raw_meta = payload.get("metadata", {})
                metadata = dict(raw_meta) if isinstance(raw_meta, dict) else {}
                has_cp_payload = "cp" in payload
                raw_cp = payload.get("cp", {})
                cp_payload = dict(raw_cp) if isinstance(raw_cp, dict) else {}
            else:
                # Backward compatibility with old snapshot format.
                state = payload
                metadata = {}
                cp_payload = {}
            self._store.load_snapshot(state)
            if has_cp_payload:
                self._load_cp_snapshot_payload(cp_payload)
            with self._consensus_lock:
                self._last_applied_index = int(metadata.get("last_applied_index", 0))
                self._next_log_index = max(self._last_applied_index + 1, 1)
                self._term = int(metadata.get("term", self._term))
            self._inc_stat("snapshot_load_success")
        except Exception:
            self._inc_stat("snapshot_load_failures")

    def _replay_wal_if_enabled(self) -> None:
        """
        Replay WAL entries after snapshot recovery.
        """
        if self._store_is_centralized:
            return
        if self._wal is None:
            return
        try:
            entries = self._wal.replay()
            sorted_entries = sorted(
                (
                    item.get("operation")
                    for item in entries
                    if isinstance(item, dict) and isinstance(item.get("operation"), dict)
                ),
                key=lambda op: int(op.get("log_index", 0)),
            )
            for payload in sorted_entries:
                log_index = int(payload.get("log_index", 0))
                if log_index <= self._last_applied_index:
                    continue
                if str(payload.get("collection", "")) == "topic":
                    continue
                self._apply_payload(payload, source="wal_replay")
                with self._consensus_lock:
                    self._last_applied_index = max(self._last_applied_index, log_index)
                    self._next_log_index = max(self._next_log_index, self._last_applied_index + 1)
            self._inc_stat("wal_replay_success")
        except Exception:
            self._inc_stat("wal_replay_failures")

    def _append_wal_entry(self, payload: dict[str, Any]) -> None:
        """
        Append operation payload to WAL when enabled.
        """
        if self._is_centralized_data_operation(str(payload.get("collection", ""))):
            return
        if self._wal is None:
            return
        if str(payload.get("collection", "")) == "topic":
            return
        try:
            self._wal.append({"operation": payload})
            self._ops_since_checkpoint += 1
            self._inc_stat("wal_append_success")
        except Exception:
            self._inc_stat("wal_append_failures")

    def _schedule_persist(self) -> None:
        """Schedule asynchronous snapshot persistence flush."""
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        self._persist_request.set()

    def _start_persistence_worker(self) -> None:
        """Start background snapshot flush worker."""
        if self._persistence is None:
            return
        self._persist_stop.clear()
        self._persist_request.clear()
        self._persist_thread = threading.Thread(
            target=self._persistence_worker_loop,
            name="cluster-persistence-worker",
            daemon=True,
        )
        self._persist_thread.start()

    def _stop_persistence_worker(self, *, flush: bool) -> None:
        """Stop persistence worker and optionally flush one final snapshot."""
        if self._persistence is None:
            return
        self._persist_stop.set()
        self._persist_request.set()
        if self._persist_thread:
            self._persist_thread.join(timeout=1.0)
            self._persist_thread = None
        if flush:
            self._save_snapshot_now()

    def _persistence_worker_loop(self) -> None:
        """Persistence worker event loop."""
        while not self._persist_stop.is_set():
            signaled = self._persist_request.wait(timeout=0.5)
            if not signaled:
                continue
            self._persist_request.clear()
            self._save_snapshot_now()

    def _save_snapshot_now(self) -> None:
        """
        Persist snapshot + metadata and checkpoint WAL when due.
        """
        if self._store_is_centralized:
            return
        if self._persistence is None:
            return
        try:
            self._persistence.save(self._snapshot_payload())
            self._inc_stat("snapshot_save_success")
            if (
                self._wal is not None
                and self._ops_since_checkpoint >= self.config.wal.checkpoint_interval_operations
            ):
                self._wal.truncate()
                self._ops_since_checkpoint = 0
        except Exception:
            self._inc_stat("snapshot_save_failures")

    def _snapshot_payload(self) -> dict[str, Any]:
        """
        Build persistence payload containing state and metadata.
        """
        with self._consensus_lock:
            metadata = {
                "term": self._term,
                "last_applied_index": self._last_applied_index,
                "node_id": self.node_id,
                "timestamp_ms": int(time.time() * 1000),
            }
        state = {} if self._store_is_centralized else self._store.create_snapshot()
        return {"state": state, "metadata": metadata, "cp": self._cp_snapshot_payload()}

    # --------------------------------------------------------------------- #
    # Consensus, split-brain protection, and leader election
    # --------------------------------------------------------------------- #

    def _start_consensus_worker(self) -> None:
        """Start election/heartbeat worker when consensus is enabled."""
        if not self.config.consensus.enabled:
            return
        self._consensus_stop.clear()
        self._consensus_thread = threading.Thread(
            target=self._consensus_loop,
            name="cluster-consensus-worker",
            daemon=True,
        )
        self._consensus_thread.start()

    def _stop_consensus_worker(self) -> None:
        """Stop consensus worker."""
        if not self.config.consensus.enabled:
            return
        self._consensus_stop.set()
        if self._consensus_thread:
            self._consensus_thread.join(timeout=1.0)
            self._consensus_thread = None

    def _consensus_loop(self) -> None:
        """
        Leader heartbeat and election loop.
        """
        next_heartbeat = time.monotonic()
        while not self._consensus_stop.is_set():
            now = time.monotonic()
            if self._is_leader():
                if now >= next_heartbeat:
                    self._send_heartbeats()
                    next_heartbeat = now + self.config.consensus.heartbeat_interval_seconds
                time.sleep(0.05)
                continue

            timeout = self.config.consensus.election_timeout_seconds * random.uniform(0.9, 1.3)
            with self._consensus_lock:
                elapsed = now - self._last_heartbeat_received
            if elapsed >= timeout:
                self._start_election()
            time.sleep(0.05)

    def _start_election(self) -> None:
        """
        Start one vote round and become leader if quorum is reached.
        """
        self._inc_stat("leader_elections_started")
        with self._consensus_lock:
            self._term += 1
            term = self._term
            self._voted_for = self.node_id
            self._leader_id = None
            self._leader_address = None
            last_log_index = self._last_applied_index

        votes = 1
        for peer in self.members():
            request = self._make_message(
                MessageKind.VOTE_REQUEST,
                {
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "address": self.config.advertise_address.as_dict(),
                },
            )
            try:
                response = request_response(
                    peer,
                    request,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                    ssl_context=self._client_ssl_context,
                    server_hostname=self.config.tls.server_hostname,
                    min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                    max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
                )
            except Exception:
                continue

            if response.get("kind") != MessageKind.VOTE_RESPONSE.value:
                continue
            payload = response.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if int(payload.get("term", 0)) > term:
                with self._consensus_lock:
                    self._term = int(payload["term"])
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_address = None
                return
            if bool(payload.get("granted")):
                votes += 1
            remote_node_id = payload.get("node_id")
            raw_addr = payload.get("address")
            if isinstance(remote_node_id, str) and isinstance(raw_addr, dict):
                try:
                    self._add_member(NodeAddress.from_dict(raw_addr), node_id=remote_node_id)
                except (TypeError, ValueError, KeyError):
                    pass

        if votes >= self._quorum_size():
            with self._consensus_lock:
                self._leader_id = self.node_id
                self._leader_address = self.config.advertise_address
                self._last_heartbeat_received = time.monotonic()
                self._last_majority_contact = time.monotonic()
            self._inc_stat("leader_elections_won")
            self._trace("election_won", term=term, votes=votes)
        else:
            with self._consensus_lock:
                self._last_heartbeat_received = time.monotonic()
            self._trace("election_lost", term=term, votes=votes)

    def _send_heartbeats(self) -> None:
        """
        Send leader heartbeats and update quorum lease state.
        """
        if not self._is_leader():
            return
        self._inc_stat("leader_heartbeats_sent")
        with self._consensus_lock:
            term = self._term
            last_applied = self._last_applied_index
        acknowledged = 1

        heartbeat = self._make_message(
            MessageKind.HEARTBEAT,
            {
                "term": term,
                "leader_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "last_applied_index": last_applied,
            },
        )
        for peer in self.members():
            try:
                response = request_response(
                    peer,
                    heartbeat,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                    ssl_context=self._client_ssl_context,
                    server_hostname=self.config.tls.server_hostname,
                    min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                    max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
                )
            except Exception:
                self._record_member_failure(peer)
                continue
            if response.get("kind") != MessageKind.HEARTBEAT_ACK.value:
                continue
            payload = response.get("payload", {})
            if isinstance(payload, dict):
                remote_node_id = payload.get("node_id")
                raw_addr = payload.get("address")
                if isinstance(remote_node_id, str) and isinstance(raw_addr, dict):
                    try:
                        self._add_member(NodeAddress.from_dict(raw_addr), node_id=remote_node_id)
                    except (TypeError, ValueError, KeyError):
                        pass
            acknowledged += 1
            self._clear_member_failure(peer)
            self._inc_stat("leader_heartbeats_acked")

        if acknowledged >= self._quorum_size():
            with self._consensus_lock:
                self._last_majority_contact = time.monotonic()

    def _is_leader(self) -> bool:
        """Return true when this node is currently leader."""
        if not self.config.consensus.enabled:
            return True
        with self._consensus_lock:
            return self._leader_id == self.node_id

    def _leader_lease_valid(self) -> bool:
        """Return true when leader still has quorum lease."""
        if not self.config.consensus.enabled:
            return True
        with self._consensus_lock:
            last_contact = self._last_majority_contact
        return (time.monotonic() - last_contact) <= self.config.consensus.leader_lease_seconds

    def _quorum_size(self) -> int:
        """Compute current majority quorum size."""
        cluster_size = len(self.members()) + 1
        return (cluster_size // 2) + 1

    def _resolve_leader_address(self) -> NodeAddress | None:
        """
        Resolve current leader endpoint for forwarded writes.
        """
        if self._is_leader():
            return self.config.advertise_address
        with self._consensus_lock:
            if self._leader_address is not None:
                return self._leader_address
            if self._leader_id is not None:
                return self._member_nodes.get(self._leader_id)
        return None

    # --------------------------------------------------------------------- #
    # Snapshot sync and protocol compatibility
    # --------------------------------------------------------------------- #

    def _handshake_peer(self, peer: NodeAddress) -> dict[str, Any] | None:
        """
        Perform handshake with peer and return response payload.
        """
        request = self._make_message(
            MessageKind.HANDSHAKE,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "protocol_min": self.config.upgrade.min_compatible_protocol_version,
                "protocol_max": self.config.upgrade.max_compatible_protocol_version,
            },
        )
        try:
            response = request_response(
                peer,
                request,
                self.config.socket_timeout_seconds,
                security_token=self.config.security.shared_token,
                ssl_context=self._client_ssl_context,
                server_hostname=self.config.tls.server_hostname,
                min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
            )
        except Exception:
            return None
        if response.get("kind") != MessageKind.HANDSHAKE_ACK.value:
            return None
        payload = response.get("payload")
        if not isinstance(payload, dict):
            return None
        peer_min = int(payload.get("protocol_min", 1))
        peer_max = int(payload.get("protocol_max", 1))
        if not self._protocol_ranges_overlap(peer_min, peer_max):
            return None
        return payload

    def _sync_from_peer_list(self, peers: list[NodeAddress]) -> None:
        """
        Request snapshot state from first responsive peer and load locally.
        """
        request = self._make_message(MessageKind.STATE_REQUEST, {})
        for peer in peers:
            try:
                response = request_response(
                    peer,
                    request,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                    ssl_context=self._client_ssl_context,
                    server_hostname=self.config.tls.server_hostname,
                    min_protocol_version=self.config.upgrade.min_compatible_protocol_version,
                    max_protocol_version=self.config.upgrade.max_compatible_protocol_version,
                )
            except Exception:
                continue
            if response.get("kind") != MessageKind.STATE_RESPONSE.value:
                continue
            payload = response.get("payload")
            if not isinstance(payload, dict):
                continue
            snapshot_payload = payload.get("snapshot")
            if not isinstance(snapshot_payload, dict):
                continue
            self._load_snapshot_payload(snapshot_payload)
            self._schedule_persist()
            self._trace("state_synced_from_peer", peer=str(peer))
            return

    def _load_snapshot_payload(self, payload: dict[str, Any]) -> None:
        """
        Load snapshot payload returned by peer state sync.
        """
        has_cp_payload = False
        if "state" in payload and isinstance(payload.get("state"), dict):
            state = dict(payload["state"])
            metadata_raw = payload.get("metadata", {})
            metadata = dict(metadata_raw) if isinstance(metadata_raw, dict) else {}
            has_cp_payload = "cp" in payload
            raw_cp = payload.get("cp", {})
            cp_payload = dict(raw_cp) if isinstance(raw_cp, dict) else {}
        else:
            state = payload
            metadata = {}
            cp_payload = {}
        if not self._store_is_centralized:
            self._store.load_snapshot(state)
        if has_cp_payload:
            self._load_cp_snapshot_payload(cp_payload)
        with self._consensus_lock:
            self._last_applied_index = int(metadata.get("last_applied_index", self._last_applied_index))
            self._next_log_index = max(self._last_applied_index + 1, self._next_log_index)
            self._term = max(self._term, int(metadata.get("term", self._term)))

    def _protocol_ranges_overlap(self, peer_min: int, peer_max: int) -> bool:
        """
        Return true when local and peer protocol compatibility ranges overlap.
        """
        local_min = self.config.upgrade.min_compatible_protocol_version
        local_max = self.config.upgrade.max_compatible_protocol_version
        return max(local_min, peer_min) <= min(local_max, peer_max)

    # --------------------------------------------------------------------- #
    # Security and ACL
    # --------------------------------------------------------------------- #

    def _is_acl_allowed(self, *, sender_node_id: str, payload: dict[str, Any]) -> bool:
        """
        Check sender-level ACL authorization for one operation payload.
        """
        if not self.config.acl.enabled:
            return True
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        action_key = f"{collection}.{action}"
        permissions = self.config.acl.sender_permissions.get(sender_node_id)
        if permissions is None:
            permissions = self.config.acl.sender_permissions.get("*")
        if permissions is None:
            return bool(self.config.acl.default_allow)
        return ("*" in permissions) or (action_key in permissions)

    def _build_server_ssl_context(self) -> ssl.SSLContext | None:
        """
        Build server SSL context when TLS is enabled.
        """
        if not self.config.tls.enabled:
            return None
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(
            certfile=str(self.config.tls.certfile),
            keyfile=str(self.config.tls.keyfile),
        )
        if self.config.tls.ca_file:
            context.load_verify_locations(cafile=str(self.config.tls.ca_file))
        if self.config.tls.require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_NONE
        context.check_hostname = False
        return context

    def _build_client_ssl_context(self) -> ssl.SSLContext | None:
        """
        Build client SSL context when TLS is enabled.
        """
        if not self.config.tls.enabled:
            return None
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.config.tls.ca_file:
            context.load_verify_locations(cafile=str(self.config.tls.ca_file))
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        if self.config.tls.certfile and self.config.tls.keyfile:
            context.load_cert_chain(
                certfile=str(self.config.tls.certfile),
                keyfile=str(self.config.tls.keyfile),
            )
        return context

    # --------------------------------------------------------------------- #
    # Observability and trace helpers
    # --------------------------------------------------------------------- #

    def _start_observability_server(self) -> None:
        """Start HTTP observability server when enabled."""
        if not self.config.observability.enable_http:
            return
        try:
            self._observability_server = ObservabilityServer(
                host=self.config.observability.host,
                port=self.config.observability.port,
                health_provider=self.health,
                metrics_provider=self.metrics_text,
                traces_provider=self.recent_traces,
            )
            self._observability_server.start()
        except Exception as exc:
            self._observability_server = None
            self._trace("observability_start_failed", reason=str(exc))

    def _stop_observability_server(self) -> None:
        """Stop observability server when running."""
        if self._observability_server is None:
            return
        self._observability_server.stop()
        self._observability_server = None

    def _trace(self, event: str, *, trace_id: str | None = None, **details: object) -> None:
        """
        Record one structured trace event in bounded memory history.
        """
        if not self.config.observability.enable_tracing:
            return
        entry = {
            "ts_ms": int(time.time() * 1000),
            "event": event,
            "node_id": self.node_id,
        }
        if trace_id:
            entry["trace_id"] = trace_id
        if details:
            entry["details"] = details
        with self._trace_lock:
            self._trace_history.append(entry)

    # --------------------------------------------------------------------- #
    # Low-level helpers
    # --------------------------------------------------------------------- #

    def _make_message(self, kind: MessageKind, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Build signed protocol envelope using local identity/security settings.
        """
        return make_message(
            kind,
            self.config.cluster_name,
            payload,
            sender_node_id=self.node_id,
            security_token=self.config.security.shared_token,
        )

    def _ensure_running(self) -> None:
        if not self.is_running:
            raise ClusterNotRunningError(
                "Cluster node is not running. Call start() before using distributed collections."
            )

    def _is_centralized_data_operation(self, collection: str) -> bool:
        """
        Return true when operation targets centralized backend state.

        Topic traffic is excluded because it is transient pub/sub fan-out, not
        persistent collection storage.
        """
        return self._store_is_centralized and collection in {"map", "list", "queue"}

    def _is_cp_operation(self, collection: str) -> bool:
        """Return true when operation belongs to the CP coordination subsystem."""
        return collection.startswith("cp_")

    def _effective_consistency_mode(self, collection: str) -> ConsistencyMode:
        """
        Resolve effective consistency mode for one collection operation.

        CP operations and (optionally) core collection mutations are always
        promoted to linearizable commits.
        """
        if self._is_cp_operation(collection):
            return ConsistencyMode.LINEARIZABLE
        if (
            self.config.cp.enabled
            and self.config.cp.mandatory_for_core_mutations
            and collection in {"map", "list", "queue", "topic"}
        ):
            return ConsistencyMode.LINEARIZABLE
        return self.config.consistency.mode

    def _cp_lock_count(self) -> int:
        """Return number of currently tracked CP locks."""
        with self._cp_state_lock:
            return len(self._cp_locks)

    def _inc_stat(self, key: str, *, delta: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + delta

    def _record_operation(self, operation_id: str) -> bool:
        """
        Record operation ID and return false when already seen.
        """
        with self._seen_lock:
            if operation_id in self._seen_operation_ids:
                return False
            self._seen_operation_ids.add(operation_id)
            self._seen_operation_order.append(operation_id)
            if len(self._seen_operation_order) > self._seen_operation_limit:
                expired = self._seen_operation_order.popleft()
                self._seen_operation_ids.discard(expired)
            return True

    def _add_member(self, address: NodeAddress, *, node_id: str | None = None) -> None:
        """
        Add/update one member endpoint and optional node-id mapping.
        """
        if address == self.config.advertise_address:
            if node_id:
                with self._members_lock:
                    self._member_nodes[node_id] = address
            return
        remote_count = 0
        with self._members_lock:
            self._members.add(address)
            remote_count = len(self._members)
            if node_id:
                self._member_nodes[node_id] = address
        with self._member_failures_lock:
            self._member_failures.pop(address, None)
        if remote_count > 0 and self.config.consensus.require_majority_for_writes:
            # Force immediate lease refresh when cluster size changes.
            with self._consensus_lock:
                if self._leader_id == self.node_id:
                    self._last_majority_contact = 0.0

    def _record_member_failure(self, peer: NodeAddress) -> None:
        """
        Track peer failures and evict when threshold is exceeded.
        """
        evict = False
        with self._member_failures_lock:
            count = self._member_failures.get(peer, 0) + 1
            self._member_failures[peer] = count
            if count >= self.config.replication.member_failure_threshold:
                self._member_failures.pop(peer, None)
                evict = True
        if evict:
            with self._members_lock:
                self._members.discard(peer)
                stale_nodes = [node_id for node_id, addr in self._member_nodes.items() if addr == peer]
                for node_id in stale_nodes:
                    self._member_nodes.pop(node_id, None)
            self._inc_stat("members_evicted")

    def _clear_member_failure(self, peer: NodeAddress) -> None:
        """Clear peer failure counter after successful delivery."""
        with self._member_failures_lock:
            self._member_failures.pop(peer, None)

    def _member_nodes_payload(self) -> list[dict[str, Any]]:
        """
        Return known node-id/address tuples for handshake metadata.
        """
        with self._members_lock:
            items = list(self._member_nodes.items())
        payload = []
        seen = set()
        for node_id, address in items:
            key = (node_id, address.host, address.port)
            if key in seen:
                continue
            seen.add(key)
            payload.append({"node_id": node_id, "address": address.as_dict()})
        if self.node_id not in {item["node_id"] for item in payload}:
            payload.append(
                {
                    "node_id": self.node_id,
                    "address": self.config.advertise_address.as_dict(),
                }
            )
        return payload

    def _member_nodes_from_payload(
        self,
        raw_members: Any,
    ) -> list[tuple[str | None, NodeAddress]]:
        """
        Parse handshake member list with backward compatibility.
        """
        if not isinstance(raw_members, list):
            return []
        parsed: list[tuple[str | None, NodeAddress]] = []
        for raw in raw_members:
            if not isinstance(raw, dict):
                continue
            if "address" in raw:
                node_id = raw.get("node_id")
                raw_address = raw.get("address")
                if isinstance(raw_address, dict):
                    try:
                        parsed.append(
                            (
                                str(node_id) if isinstance(node_id, str) and node_id else None,
                                NodeAddress.from_dict(raw_address),
                            )
                        )
                    except (TypeError, ValueError, KeyError):
                        continue
                continue
            # Backward compatibility: plain address object.
            try:
                parsed.append((None, NodeAddress.from_dict(raw)))
            except (TypeError, ValueError, KeyError):
                continue
        return parsed

    # --------------------------------------------------------------------- #
    # CP subsystem internals (distributed lock state machine)
    # --------------------------------------------------------------------- #

    def _normalize_cp_lease_ms(self, lease_seconds: float | None) -> int:
        """Normalize requested lock lease to bounded milliseconds."""
        seconds = (
            self.config.cp.lock_default_lease_seconds
            if lease_seconds is None
            else float(lease_seconds)
        )
        if seconds <= 0:
            raise ValueError("Lock lease must be > 0 seconds.")
        seconds = min(seconds, self.config.cp.lock_max_lease_seconds)
        return max(1, int(seconds * 1000))

    def _expire_cp_lock_unlocked(self, lock_name: str, now_ms: int) -> None:
        """Drop lock state when lease has expired (caller must hold CP state lock)."""
        state = self._cp_locks.get(lock_name)
        if state is None:
            return
        if int(state.get("lease_deadline_ms", 0)) > now_ms:
            return
        self._cp_locks.pop(lock_name, None)
        self._inc_stat("cp_lock_expired")

    def _apply_cp_lock_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Apply one CP lock operation deterministically in replicated log order.
        """
        action = str(payload.get("action", ""))
        lock_name = str(payload.get("name", ""))
        owner_id = str(payload.get("owner_id", ""))
        now_ms = int(payload.get("now_ms", int(time.time() * 1000)))
        lease_ms = int(payload.get("lease_ms", 0))
        token = str(payload.get("token", ""))

        with self._cp_state_lock:
            self._expire_cp_lock_unlocked(lock_name, now_ms)
            state = self._cp_locks.get(lock_name)

            if action == "acquire":
                if not owner_id:
                    self._inc_stat("cp_lock_acquire_conflicts")
                    return {"acquired": False, "reason": "missing owner_id"}
                if lease_ms <= 0:
                    lease_ms = self._normalize_cp_lease_ms(None)
                if state is None:
                    fencing = int(self._cp_lock_fencing_counters.get(lock_name, 0)) + 1
                    self._cp_lock_fencing_counters[lock_name] = fencing
                    granted_token = token or str(payload.get("op_id", "")) or uuid.uuid4().hex
                    self._cp_locks[lock_name] = {
                        "owner_id": owner_id,
                        "token": granted_token,
                        "fencing_token": fencing,
                        "lease_deadline_ms": now_ms + lease_ms,
                        "reentrancy": 1,
                        "updated_at_ms": now_ms,
                    }
                    self._inc_stat("cp_lock_acquire_success")
                    return {
                        "acquired": True,
                        "owner_id": owner_id,
                        "token": granted_token,
                        "fencing_token": fencing,
                        "lease_deadline_ms": now_ms + lease_ms,
                        "reentrancy": 1,
                    }

                if (
                    self.config.cp.allow_reentrant_locks
                    and str(state.get("owner_id", "")) == owner_id
                    and str(state.get("token", "")) == token
                ):
                    reentrancy = int(state.get("reentrancy", 1)) + 1
                    state["reentrancy"] = reentrancy
                    state["lease_deadline_ms"] = now_ms + lease_ms
                    state["updated_at_ms"] = now_ms
                    self._inc_stat("cp_lock_acquire_success")
                    return {
                        "acquired": True,
                        "owner_id": owner_id,
                        "token": str(state["token"]),
                        "fencing_token": int(state["fencing_token"]),
                        "lease_deadline_ms": int(state["lease_deadline_ms"]),
                        "reentrancy": reentrancy,
                    }

                self._inc_stat("cp_lock_acquire_conflicts")
                return {
                    "acquired": False,
                    "owner_id": str(state.get("owner_id", "")),
                    "fencing_token": int(state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                }

            if action == "release":
                if state is None:
                    self._inc_stat("cp_lock_release_failures")
                    return {"released": False}
                if str(state.get("owner_id", "")) != owner_id or str(state.get("token", "")) != token:
                    self._inc_stat("cp_lock_release_failures")
                    return {"released": False}

                reentrancy = int(state.get("reentrancy", 1))
                if self.config.cp.allow_reentrant_locks and reentrancy > 1:
                    state["reentrancy"] = reentrancy - 1
                    state["updated_at_ms"] = now_ms
                    self._inc_stat("cp_lock_release_success")
                    return {"released": True, "remaining_reentrancy": reentrancy - 1}

                self._cp_locks.pop(lock_name, None)
                self._inc_stat("cp_lock_release_success")
                return {"released": True, "remaining_reentrancy": 0}

            if action == "refresh":
                if state is None:
                    self._inc_stat("cp_lock_refresh_failures")
                    return {"refreshed": False}
                if str(state.get("owner_id", "")) != owner_id or str(state.get("token", "")) != token:
                    self._inc_stat("cp_lock_refresh_failures")
                    return {"refreshed": False}
                if lease_ms <= 0:
                    lease_ms = self._normalize_cp_lease_ms(None)
                state["lease_deadline_ms"] = now_ms + lease_ms
                state["updated_at_ms"] = now_ms
                self._inc_stat("cp_lock_refresh_success")
                return {"refreshed": True, "lease_deadline_ms": int(state["lease_deadline_ms"])}

        raise ValueError(f"Unsupported cp_lock action: {action!r}")

    def _cp_lock_view(self, lock_name: str) -> dict[str, Any]:
        """Read local CP lock state view with lease-expiration cleanup."""
        now_ms = int(time.time() * 1000)
        with self._cp_state_lock:
            self._expire_cp_lock_unlocked(lock_name, now_ms)
            state = self._cp_locks.get(lock_name)
            if state is None:
                return {"locked": False}
            return {
                "locked": True,
                "owner_id": str(state.get("owner_id", "")),
                "token": str(state.get("token", "")),
                "fencing_token": int(state.get("fencing_token", 0)),
                "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                "reentrancy": int(state.get("reentrancy", 1)),
            }

    def _cp_lock_acquire(
        self,
        lock_name: str,
        *,
        owner_id: str,
        blocking: bool,
        timeout_seconds: float | None,
        lease_seconds: float | None,
        current_token: str | None = None,
    ) -> dict[str, Any]:
        """Acquire CP lock, optionally retrying until timeout."""
        if not self.config.cp.enabled:
            raise ClusterNotRunningError("CP subsystem is disabled.")
        lease_ms = self._normalize_cp_lease_ms(lease_seconds)
        deadline: float | None = None
        if blocking and timeout_seconds is not None:
            deadline = time.monotonic() + max(0.0, float(timeout_seconds))

        token = current_token or uuid.uuid4().hex
        while True:
            result = self._submit_local_operation(
                collection="cp_lock",
                name=lock_name,
                action="acquire",
                values={
                    "owner_id": owner_id,
                    "token": token,
                    "lease_ms": lease_ms,
                    "now_ms": int(time.time() * 1000),
                },
            )
            if isinstance(result, dict) and bool(result.get("acquired")):
                return result
            if not blocking:
                return {"acquired": False}
            if deadline is not None and time.monotonic() >= deadline:
                return {"acquired": False}
            time.sleep(self.config.cp.lock_retry_interval_seconds)

    def _cp_lock_release(self, lock_name: str, *, owner_id: str, token: str) -> bool:
        """Release CP lock ownership."""
        if not self.config.cp.enabled:
            return False
        result = self._submit_local_operation(
            collection="cp_lock",
            name=lock_name,
            action="release",
            values={"owner_id": owner_id, "token": token, "now_ms": int(time.time() * 1000)},
        )
        return bool(isinstance(result, dict) and result.get("released"))

    def _cp_lock_refresh(
        self,
        lock_name: str,
        *,
        owner_id: str,
        token: str,
        lease_seconds: float | None,
    ) -> bool:
        """Extend CP lock lease for active owner session."""
        if not self.config.cp.enabled:
            return False
        lease_ms = self._normalize_cp_lease_ms(lease_seconds)
        result = self._submit_local_operation(
            collection="cp_lock",
            name=lock_name,
            action="refresh",
            values={
                "owner_id": owner_id,
                "token": token,
                "lease_ms": lease_ms,
                "now_ms": int(time.time() * 1000),
            },
        )
        return bool(isinstance(result, dict) and result.get("refreshed"))

    def _cp_lock_is_locked(self, lock_name: str) -> bool:
        """Return local view of lock ownership state."""
        return bool(self._cp_lock_view(lock_name).get("locked"))

    def _cp_lock_owner(self, lock_name: str) -> str | None:
        """Return local view of lock owner ID."""
        view = self._cp_lock_view(lock_name)
        owner_id = view.get("owner_id")
        return str(owner_id) if isinstance(owner_id, str) and owner_id else None

    def _cp_snapshot_payload(self) -> dict[str, Any]:
        """Build serializable snapshot for CP lock subsystem state."""
        with self._cp_state_lock:
            locks = {
                name: {
                    "owner_id": str(state.get("owner_id", "")),
                    "token": str(state.get("token", "")),
                    "fencing_token": int(state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                    "reentrancy": int(state.get("reentrancy", 1)),
                    "updated_at_ms": int(state.get("updated_at_ms", 0)),
                }
                for name, state in self._cp_locks.items()
            }
            counters = {
                name: int(counter) for name, counter in self._cp_lock_fencing_counters.items()
            }
        return {"locks": locks, "fencing_counters": counters}

    def _load_cp_snapshot_payload(self, payload: dict[str, Any]) -> None:
        """Load CP lock snapshot payload into runtime memory."""
        raw_locks = payload.get("locks", {})
        raw_counters = payload.get("fencing_counters", {})
        parsed_locks: dict[str, dict[str, Any]] = {}
        if isinstance(raw_locks, dict):
            for name, raw_state in raw_locks.items():
                if not isinstance(raw_state, dict):
                    continue
                parsed_locks[str(name)] = {
                    "owner_id": str(raw_state.get("owner_id", "")),
                    "token": str(raw_state.get("token", "")),
                    "fencing_token": int(raw_state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(raw_state.get("lease_deadline_ms", 0)),
                    "reentrancy": max(1, int(raw_state.get("reentrancy", 1))),
                    "updated_at_ms": int(raw_state.get("updated_at_ms", 0)),
                }
        parsed_counters: dict[str, int] = {}
        if isinstance(raw_counters, dict):
            for name, counter in raw_counters.items():
                try:
                    parsed_counters[str(name)] = int(counter)
                except (TypeError, ValueError):
                    continue
        with self._cp_state_lock:
            self._cp_locks = parsed_locks
            self._cp_lock_fencing_counters = parsed_counters

    # --------------------------------------------------------------------- #
    # Distributed topic internals
    # --------------------------------------------------------------------- #

    def _dispatch_topic(self, topic_name: str, message: Any) -> None:
        """
        Deliver one topic message to all local subscribers.
        """
        with self._topic_lock:
            callbacks = list(self._topic_subscribers.get(topic_name, {}).values())
        for callback in callbacks:
            try:
                callback(message)
            except Exception:
                continue

    def _topic_subscribe(self, topic_name: str, callback: Callable[[Any], None]) -> str:
        """
        Register local topic callback and return subscription ID.
        """
        subscription_id = uuid.uuid4().hex
        with self._topic_lock:
            registry = self._topic_subscribers.setdefault(topic_name, {})
            registry[subscription_id] = callback
        return subscription_id

    def _topic_unsubscribe(self, topic_name: str, subscription_id: str) -> bool:
        """
        Remove topic callback by subscription ID.
        """
        with self._topic_lock:
            registry = self._topic_subscribers.get(topic_name)
            if not registry:
                return False
            return registry.pop(subscription_id, None) is not None

    def _topic_publish(self, topic_name: str, message: Any) -> None:
        """Publish topic message under configured consistency strategy."""
        self._submit_local_operation(
            collection="topic",
            name=topic_name,
            action="publish",
            values={"message": message},
        )

    # --------------------------------------------------------------------- #
    # Primitive adapter methods used by distributed wrappers
    # --------------------------------------------------------------------- #

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
