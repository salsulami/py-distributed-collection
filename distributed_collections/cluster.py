"""
Cluster node runtime for distributed collection replication.

This module hosts the primary orchestration class, :class:`ClusterNode`.
The node coordinates:

* discovery and peer membership updates
* request/response message handling over TCP
* asynchronous replication with retry/backoff
* optional snapshot persistence for restart recovery
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from typing import Any

from .config import ClusterConfig, DiscoveryMode, NodeAddress
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
from .persistence import SnapshotPersistence
from .primitives import DistributedList, DistributedMap, DistributedQueue, DistributedTopic
from .protocol import (
    MessageKind,
    assert_authenticated,
    assert_protocol_compatible,
    make_message,
)
from .store import ClusterDataStore
from .transport import TcpTransportServer, request_response

_REPLICATION_SENTINEL: tuple[NodeAddress, dict[str, Any], int] | None = None


class ClusterNode:
    """
    Runtime container for distributed collections in one Python process.

    The node exposes distributed data structures (map/list/queue/topic), accepts
    inbound TCP operations from peers, and asynchronously replicates local
    mutations to known members.
    """

    def __init__(self, config: ClusterConfig) -> None:
        """
        Initialize a cluster node with immutable startup configuration.

        Parameters
        ----------
        config:
            Runtime settings controlling transport, discovery, security,
            replication retry policy, and persistence behavior.
        """
        self.config = config
        self.node_id = uuid.uuid4().hex
        self._store = ClusterDataStore()
        self._members: set[NodeAddress] = set()
        self._members_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._running = False

        self._transport = TcpTransportServer(
            bind=self.config.bind,
            message_handler=self._handle_transport_message,
        )
        self._discovery_responder: MulticastDiscoveryResponder | None = None

        self._seen_operation_ids: set[str] = set()
        self._seen_operation_order: deque[str] = deque()
        self._seen_operation_limit = 20_000
        self._seen_lock = threading.Lock()

        self._topic_subscribers: dict[str, dict[str, Callable[[Any], None]]] = {}
        self._topic_lock = threading.RLock()

        self._map_handles: dict[str, DistributedMap] = {}
        self._list_handles: dict[str, DistributedList] = {}
        self._queue_handles: dict[str, DistributedQueue] = {}
        self._topic_handles: dict[str, DistributedTopic] = {}
        self._handle_lock = threading.RLock()

        self._replication_queue: queue.Queue[
            tuple[NodeAddress, dict[str, Any], int] | None
        ] = queue.Queue()
        self._replication_stop = threading.Event()
        self._replication_threads: list[threading.Thread] = []
        self._member_failures: dict[NodeAddress, int] = {}
        self._member_failures_lock = threading.RLock()

        self._persistence: SnapshotPersistence | None = None
        if self.config.persistence.enabled:
            self._persistence = SnapshotPersistence(
                self.config.persistence.snapshot_path,
                fsync=self.config.persistence.fsync,
            )
        self._persist_request = threading.Event()
        self._persist_stop = threading.Event()
        self._persist_thread: threading.Thread | None = None

        self._stats_lock = threading.Lock()
        self._stats: dict[str, int] = {
            "local_mutations": 0,
            "remote_mutations": 0,
            "replication_enqueued": 0,
            "replication_success": 0,
            "replication_failures": 0,
            "snapshot_load_success": 0,
            "snapshot_load_failures": 0,
            "snapshot_save_success": 0,
            "snapshot_save_failures": 0,
            "auth_failures": 0,
            "protocol_failures": 0,
            "dropped_messages": 0,
            "members_evicted": 0,
        }

    def start(self, *, join: bool = True) -> None:
        """
        Start transport/runtime workers and optionally join cluster peers.

        Parameters
        ----------
        join:
            If true, run one immediate discovery and handshake cycle.
        """
        with self._lifecycle_lock:
            if self._running:
                return

            self._load_snapshot_if_enabled()
            self._transport.start()
            self._start_replication_workers()
            self._start_persistence_worker()

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

        if join:
            self.join_cluster()

    def stop(self) -> None:
        """
        Stop network listeners and background workers.

        If persistence is enabled, the method flushes one final snapshot.
        """
        with self._lifecycle_lock:
            if not self._running:
                return
            self._running = False

            if self._discovery_responder:
                self._discovery_responder.stop()
                self._discovery_responder = None

            self._stop_replication_workers()
            self._stop_persistence_worker(flush=True)
            self._transport.stop()

    def close(self) -> None:
        """Alias for :meth:`stop` for context-manager style usage."""
        self.stop()

    @property
    def is_running(self) -> bool:
        """Return whether the node currently accepts network traffic."""
        with self._lifecycle_lock:
            return self._running

    def join_cluster(self) -> list[NodeAddress]:
        """
        Discover and handshake with peers, then optionally sync full state.

        Returns
        -------
        list[NodeAddress]
            Addresses discovered during this join cycle.
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
            self._add_member(peer)
            for member in self._addresses_from_payload(payload.get("members", [])):
                self._add_member(member)

        if self.config.auto_sync_on_join and reachable_peers:
            self._sync_from_peer_list(reachable_peers)
        return discovered

    def members(self) -> list[NodeAddress]:
        """
        Return known peer addresses excluding this node.
        """
        with self._members_lock:
            ordered = sorted(self._members, key=lambda address: (address.host, address.port))
        return ordered

    def wait_for_next_join_window(self) -> None:
        """
        Sleep for the configured reconnect interval.
        """
        time.sleep(float(self.config.reconnect_interval_seconds))

    def stats(self) -> dict[str, Any]:
        """
        Return runtime counters for operational observability.

        Returns
        -------
        dict[str, Any]
            Dictionary containing cumulative counters and live queue depth.
        """
        with self._stats_lock:
            counters = dict(self._stats)
        counters["member_count"] = len(self.members())
        counters["replication_queue_depth"] = self._replication_queue.qsize()
        counters["running"] = self.is_running
        return counters

    def get_map(self, name: str) -> DistributedMap:
        """
        Return a distributed map handle with the given logical name.
        """
        with self._handle_lock:
            handle = self._map_handles.get(name)
            if handle is None:
                handle = DistributedMap(self, name)
                self._map_handles[name] = handle
            return handle

    def get_list(self, name: str) -> DistributedList:
        """Return a distributed list handle by name."""
        with self._handle_lock:
            handle = self._list_handles.get(name)
            if handle is None:
                handle = DistributedList(self, name)
                self._list_handles[name] = handle
            return handle

    def get_queue(self, name: str) -> DistributedQueue:
        """Return a distributed queue handle by name."""
        with self._handle_lock:
            handle = self._queue_handles.get(name)
            if handle is None:
                handle = DistributedQueue(self, name)
                self._queue_handles[name] = handle
            return handle

    def get_topic(self, name: str) -> DistributedTopic:
        """Return a distributed topic handle by name."""
        with self._handle_lock:
            handle = self._topic_handles.get(name)
            if handle is None:
                handle = DistributedTopic(self, name)
                self._topic_handles[name] = handle
            return handle

    def _make_message(self, kind: MessageKind, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Build signed protocol message using local cluster/security settings.
        """
        return make_message(
            kind,
            self.config.cluster_name,
            payload,
            sender_node_id=self.node_id,
            security_token=self.config.security.shared_token,
        )

    def _inc_stat(self, key: str, delta: int = 1) -> None:
        """Increment one runtime counter."""
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + delta

    def _ensure_running(self) -> None:
        if not self.is_running:
            raise ClusterNotRunningError(
                "Cluster node is not running. Call start() before mutating distributed collections."
            )

    def _add_member(self, address: NodeAddress) -> None:
        """
        Add one peer address to membership set if it is not local self.
        """
        if address == self.config.advertise_address:
            return
        with self._members_lock:
            self._members.add(address)
        with self._member_failures_lock:
            self._member_failures.pop(address, None)

    def _member_payload(self) -> list[dict[str, object]]:
        """
        Return known members plus self address as dictionaries.
        """
        members = self.members()
        members.append(self.config.advertise_address)
        unique: dict[tuple[str, int], NodeAddress] = {}
        for address in members:
            unique[(address.host, address.port)] = address
        return [address.as_dict() for address in unique.values()]

    def _addresses_from_payload(self, raw_members: Any) -> list[NodeAddress]:
        """
        Parse payload member objects into validated addresses.
        """
        if not isinstance(raw_members, list):
            return []
        parsed: list[NodeAddress] = []
        for raw in raw_members:
            if not isinstance(raw, dict):
                continue
            try:
                parsed.append(NodeAddress.from_dict(raw))
            except (KeyError, TypeError, ValueError):
                continue
        return parsed

    def _handshake_peer(self, peer: NodeAddress) -> dict[str, Any] | None:
        """
        Send handshake message to one peer and return response payload.
        """
        request = self._make_message(
            MessageKind.HANDSHAKE,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )
        try:
            response = request_response(
                peer,
                request,
                self.config.socket_timeout_seconds,
                security_token=self.config.security.shared_token,
            )
        except Exception:
            return None
        if response.get("kind") != MessageKind.HANDSHAKE_ACK.value:
            return None
        payload = response.get("payload")
        if isinstance(payload, dict):
            return payload
        return None

    def _sync_from_peer_list(self, peers: list[NodeAddress]) -> None:
        """
        Request and load a full snapshot from the first responsive peer.
        """
        request = self._make_message(MessageKind.STATE_REQUEST, {})
        for peer in peers:
            try:
                response = request_response(
                    peer,
                    request,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                )
            except Exception:
                continue
            if response.get("kind") != MessageKind.STATE_RESPONSE.value:
                continue
            payload = response.get("payload")
            if not isinstance(payload, dict):
                continue
            snapshot = payload.get("snapshot")
            if not isinstance(snapshot, dict):
                continue
            self._store.load_snapshot(snapshot)
            self._schedule_persist()
            return

    def _record_operation(self, operation_id: str) -> bool:
        """
        Track operation IDs for de-duplication.

        Returns
        -------
        bool
            ``True`` if operation ID was new and recorded.
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

    def _enqueue_replication(self, envelope: dict[str, Any]) -> None:
        """
        Fan out one protocol message to all members via async workers.
        """
        for peer in self.members():
            self._replication_queue.put((peer, envelope, 1))
            self._inc_stat("replication_enqueued")

    def _record_member_failure(self, peer: NodeAddress) -> None:
        """
        Track peer delivery failures and evict unstable members.
        """
        evicted = False
        with self._member_failures_lock:
            count = self._member_failures.get(peer, 0) + 1
            self._member_failures[peer] = count
            if count >= self.config.replication.member_failure_threshold:
                self._member_failures.pop(peer, None)
                evicted = True
        if evicted:
            with self._members_lock:
                self._members.discard(peer)
            self._inc_stat("members_evicted")

    def _clear_member_failure(self, peer: NodeAddress) -> None:
        """Clear peer failure counter after successful communication."""
        with self._member_failures_lock:
            self._member_failures.pop(peer, None)

    def _start_replication_workers(self) -> None:
        """Start background replication worker threads."""
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
        """Stop replication workers and wait briefly for shutdown."""
        self._replication_stop.set()
        for _ in self._replication_threads:
            self._replication_queue.put(_REPLICATION_SENTINEL)
        for thread in self._replication_threads:
            thread.join(timeout=1.0)
        self._replication_threads.clear()

    def _replication_worker_loop(self) -> None:
        """
        Deliver enqueued replication messages with retry/backoff.
        """
        while not self._replication_stop.is_set():
            task = self._replication_queue.get()
            if task is _REPLICATION_SENTINEL:
                return

            peer, envelope, attempt = task
            try:
                response = request_response(
                    peer,
                    envelope,
                    self.config.socket_timeout_seconds,
                    security_token=self.config.security.shared_token,
                )
                if response.get("kind") == MessageKind.ERROR.value:
                    raise OSError(f"Peer returned error: {response!r}")
                self._clear_member_failure(peer)
                self._inc_stat("replication_success")
            except Exception:
                self._inc_stat("replication_failures")
                self._record_member_failure(peer)
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
                self._replication_queue.put((peer, envelope, attempt + 1))

    def _load_snapshot_if_enabled(self) -> None:
        """
        Load persisted snapshot from disk before networking starts.
        """
        if self._persistence is None:
            return
        try:
            snapshot = self._persistence.load()
            if snapshot is None:
                return
            self._store.load_snapshot(snapshot)
            self._inc_stat("snapshot_load_success")
        except Exception:
            self._inc_stat("snapshot_load_failures")

    def _save_snapshot_now(self) -> None:
        """
        Persist a full snapshot immediately when persistence is enabled.
        """
        if self._persistence is None:
            return
        try:
            self._persistence.save(self._store.create_snapshot())
            self._inc_stat("snapshot_save_success")
        except Exception:
            self._inc_stat("snapshot_save_failures")

    def _schedule_persist(self) -> None:
        """
        Coalesce snapshot persistence requests from mutation hot paths.
        """
        if self._persistence is None:
            return
        self._persist_request.set()

    def _start_persistence_worker(self) -> None:
        """
        Start background snapshot flush worker when persistence is enabled.
        """
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
        """
        Stop persistence worker and optionally flush final snapshot.
        """
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
        """
        Wait for persistence requests and flush snapshots in background.
        """
        while not self._persist_stop.is_set():
            requested = self._persist_request.wait(timeout=0.5)
            if not requested:
                continue
            self._persist_request.clear()
            self._save_snapshot_now()

    def _submit_local_operation(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> Any:
        """
        Apply local mutation and asynchronously replicate to peers.

        Returns
        -------
        Any
            Local return value from the data store.
        """
        self._ensure_running()
        payload: dict[str, Any] = {
            "op_id": uuid.uuid4().hex,
            "origin_node_id": self.node_id,
            "collection": collection,
            "name": name,
            "action": action,
        }
        if values:
            payload.update(values)

        self._record_operation(payload["op_id"])

        if collection == "topic":
            if action == "publish":
                self._dispatch_topic(name, payload.get("message"))
                self._enqueue_replication(self._make_message(MessageKind.OPERATION, payload))
                self._inc_stat("local_mutations")
            return None

        result = self._store.apply_mutation(payload)
        self._enqueue_replication(self._make_message(MessageKind.OPERATION, payload))
        self._inc_stat("local_mutations")
        self._schedule_persist()
        return result

    def _handle_transport_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        Process one inbound transport message and return protocol response.
        """
        try:
            assert_protocol_compatible(message)
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

        cluster = message.get("cluster")
        if cluster != self.config.cluster_name:
            self._inc_stat("dropped_messages")
            return self._make_message(MessageKind.ERROR, {"reason": "cluster mismatch"})

        kind = message.get("kind")
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}

        if kind == MessageKind.HANDSHAKE.value:
            return self._handle_handshake(payload)
        if kind == MessageKind.STATE_REQUEST.value:
            return self._make_message(
                MessageKind.STATE_RESPONSE,
                {"snapshot": self._store.create_snapshot()},
            )
        if kind == MessageKind.OPERATION.value:
            self._handle_remote_operation(payload)
            return self._make_message(MessageKind.OPERATION, {"status": "ok"})
        return self._make_message(
            MessageKind.ERROR,
            {"reason": f"unsupported message kind: {kind!r}"},
        )

    def _handle_handshake(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Register peer from handshake request and return known members.
        """
        raw_address = payload.get("address")
        if isinstance(raw_address, dict):
            try:
                self._add_member(NodeAddress.from_dict(raw_address))
            except (KeyError, TypeError, ValueError):
                pass
        return self._make_message(
            MessageKind.HANDSHAKE_ACK,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
                "members": self._member_payload(),
            },
        )

    def _handle_remote_operation(self, payload: dict[str, Any]) -> None:
        """
        Apply replicated operation received from another node.
        """
        operation_id = str(payload.get("op_id", ""))
        if not operation_id:
            return
        if not self._record_operation(operation_id):
            return

        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        name = str(payload.get("name", ""))

        if collection == "topic":
            if action == "publish":
                self._dispatch_topic(name, payload.get("message"))
                self._inc_stat("remote_mutations")
            return

        self._store.apply_mutation(payload)
        self._inc_stat("remote_mutations")
        self._schedule_persist()

    def _dispatch_topic(self, topic_name: str, message: Any) -> None:
        """
        Invoke all subscriber callbacks for one topic publication.
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
        Register topic callback and return subscription ID.
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
        """Publish one topic message locally and replicate to peers."""
        self._submit_local_operation(
            collection="topic",
            name=topic_name,
            action="publish",
            values={"message": message},
        )

    def _map_put(self, map_name: str, key: str, value: Any) -> Any:
        """Internal mutator used by :class:`DistributedMap`."""
        return self._submit_local_operation(
            collection="map",
            name=map_name,
            action="put",
            values={"key": key, "value": value},
        )

    def _map_get(self, map_name: str, key: str, default: Any = None) -> Any:
        """Internal map read helper."""
        return self._store.map_get(map_name, key, default)

    def _map_remove(self, map_name: str, key: str) -> Any:
        """Internal map delete helper."""
        return self._submit_local_operation(
            collection="map",
            name=map_name,
            action="remove",
            values={"key": key},
        )

    def _map_clear(self, map_name: str) -> None:
        """Internal map clear helper."""
        self._submit_local_operation(
            collection="map",
            name=map_name,
            action="clear",
        )

    def _map_items(self, map_name: str) -> dict[str, Any]:
        """Return map snapshot dictionary."""
        return self._store.map_items(map_name)

    def _map_size(self, map_name: str) -> int:
        """Return current map size."""
        return self._store.map_size(map_name)

    def _map_contains(self, map_name: str, key: str) -> bool:
        """Return true if key exists in map."""
        return self._store.map_contains(map_name, key)

    def _list_append(self, list_name: str, value: Any) -> None:
        """Internal list append helper."""
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="append",
            values={"value": value},
        )

    def _list_extend(self, list_name: str, values: list[Any]) -> None:
        """Internal list extend helper."""
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="extend",
            values={"values": values},
        )

    def _list_set(self, list_name: str, index: int, value: Any) -> Any:
        """Internal list index assignment helper."""
        return self._submit_local_operation(
            collection="list",
            name=list_name,
            action="set",
            values={"index": index, "value": value},
        )

    def _list_pop(self, list_name: str, index: int | None = None) -> Any:
        """Internal list pop helper."""
        return self._submit_local_operation(
            collection="list",
            name=list_name,
            action="pop",
            values={"index": index},
        )

    def _list_remove_value(self, list_name: str, value: Any) -> bool:
        """Internal list remove-by-value helper."""
        return bool(
            self._submit_local_operation(
                collection="list",
                name=list_name,
                action="remove_value",
                values={"value": value},
            )
        )

    def _list_clear(self, list_name: str) -> None:
        """Internal list clear helper."""
        self._submit_local_operation(
            collection="list",
            name=list_name,
            action="clear",
        )

    def _list_values(self, list_name: str) -> list[Any]:
        """Return list snapshot values."""
        return self._store.list_values(list_name)

    def _list_size(self, list_name: str) -> int:
        """Return list length."""
        return self._store.list_size(list_name)

    def _queue_offer(self, queue_name: str, value: Any) -> None:
        """Internal queue enqueue helper."""
        self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="offer",
            values={"value": value},
        )

    def _queue_poll(self, queue_name: str) -> Any:
        """Internal queue dequeue helper."""
        return self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="poll",
        )

    def _queue_clear(self, queue_name: str) -> None:
        """Internal queue clear helper."""
        self._submit_local_operation(
            collection="queue",
            name=queue_name,
            action="clear",
        )

    def _queue_peek(self, queue_name: str) -> Any:
        """Return queue head without mutating queue."""
        return self._store.queue_peek(queue_name)

    def _queue_values(self, queue_name: str) -> list[Any]:
        """Return queue values from head to tail."""
        return self._store.queue_values(queue_name)

    def _queue_size(self, queue_name: str) -> int:
        """Return queue size."""
        return self._store.queue_size(queue_name)
