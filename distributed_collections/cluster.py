"""
Cluster node runtime for distributed collection replication.

This module contains the primary orchestration class, :class:`ClusterNode`,
which owns networking, peer discovery, membership tracking, and collection
mutation replication.
"""

from __future__ import annotations

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
from .exceptions import ClusterNotRunningError
from .primitives import DistributedList, DistributedMap, DistributedQueue, DistributedTopic
from .protocol import MessageKind, make_message
from .store import ClusterDataStore
from .transport import TcpTransportServer, request_response


class ClusterNode:
    """
    Runtime container for distributed collections on one Python process.

    A node exposes distributed data structures (map/list/queue/topic), accepts
    inbound TCP operations from peers, and replicates local mutations to known
    members. Discovery can use static seed peers, multicast, or both.
    """

    def __init__(self, config: ClusterConfig) -> None:
        """
        Initialize a cluster node with immutable startup configuration.

        Parameters
        ----------
        config:
            Cluster runtime settings controlling network binding, discovery
            strategies, and timeout values.
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

    def start(self, *, join: bool = True) -> None:
        """
        Start the TCP listener, optional multicast responder, and join process.

        Parameters
        ----------
        join:
            If true, the node runs immediate peer discovery and handshake after
            startup.
        """
        with self._lifecycle_lock:
            if self._running:
                return
            self._transport.start()
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
        Stop the node and close network resources.

        Collection data remains in memory in the current process; this method
        only affects networking/runtime behavior.
        """
        with self._lifecycle_lock:
            if not self._running:
                return
            if self._discovery_responder:
                self._discovery_responder.stop()
                self._discovery_responder = None
            self._transport.stop()
            self._running = False

    def close(self) -> None:
        """Alias for :meth:`stop` for context-manager-like usage patterns."""
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
            Addresses that were discovered in this join cycle.
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
        Return known member addresses excluding local node address.

        Membership is best-effort and eventually consistent.
        """
        with self._members_lock:
            ordered = sorted(self._members, key=lambda a: (a.host, a.port))
        return ordered

    def wait_for_next_join_window(self) -> None:
        """
        Sleep for the configured reconnect interval.

        This helper is useful for user-managed reconnection loops.
        """
        time.sleep(float(self.config.reconnect_interval_seconds))

    def get_map(self, name: str) -> DistributedMap:
        """
        Return a distributed map handle with the provided logical name.

        The same object instance is reused per name for convenience.
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

    def _ensure_running(self) -> None:
        if not self.is_running:
            raise ClusterNotRunningError(
                "Cluster node is not running. Call start() before mutating distributed collections."
            )

    def _add_member(self, address: NodeAddress) -> None:
        """
        Add one peer address to membership list if it is not local self.
        """
        if address == self.config.advertise_address:
            return
        with self._members_lock:
            self._members.add(address)

    def _member_payload(self) -> list[dict[str, object]]:
        """
        Return known members plus self address as serializable dictionaries.
        """
        members = self.members()
        members.append(self.config.advertise_address)
        unique: dict[tuple[str, int], NodeAddress] = {}
        for address in members:
            unique[(address.host, address.port)] = address
        return [address.as_dict() for address in unique.values()]

    def _addresses_from_payload(self, raw_members: Any) -> list[NodeAddress]:
        """
        Convert payload member list into validated addresses.
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
        Send handshake message to peer and return payload when successful.
        """
        request = make_message(
            MessageKind.HANDSHAKE,
            self.config.cluster_name,
            {
                "node_id": self.node_id,
                "address": self.config.advertise_address.as_dict(),
            },
        )
        try:
            response = request_response(peer, request, self.config.socket_timeout_seconds)
        except OSError:
            return None
        if response.get("kind") != MessageKind.HANDSHAKE_ACK.value:
            return None
        payload = response.get("payload")
        if isinstance(payload, dict):
            return payload
        return None

    def _sync_from_peer_list(self, peers: list[NodeAddress]) -> None:
        """
        Request and load state snapshot from the first responsive peer.
        """
        request = make_message(MessageKind.STATE_REQUEST, self.config.cluster_name, {})
        for peer in peers:
            try:
                response = request_response(peer, request, self.config.socket_timeout_seconds)
            except OSError:
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

    def _broadcast_operation(self, operation_payload: dict[str, Any]) -> None:
        """
        Forward one operation payload to all known members via TCP.
        """
        envelope = make_message(
            MessageKind.OPERATION,
            self.config.cluster_name,
            operation_payload,
        )
        for peer in self.members():
            try:
                request_response(peer, envelope, self.config.socket_timeout_seconds)
            except OSError:
                continue

    def _submit_local_operation(
        self,
        *,
        collection: str,
        name: str,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> Any:
        """
        Apply a local mutation and replicate it to peers.

        Returns
        -------
        Any
            Local apply return value from the underlying data store.
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
                self._broadcast_operation(payload)
            return None

        result = self._store.apply_mutation(payload)
        self._broadcast_operation(payload)
        return result

    def _handle_transport_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        Process one inbound transport message and return protocol response.
        """
        cluster = message.get("cluster")
        if cluster != self.config.cluster_name:
            return make_message(
                MessageKind.ERROR,
                self.config.cluster_name,
                {"reason": "cluster mismatch"},
            )

        kind = message.get("kind")
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}

        if kind == MessageKind.HANDSHAKE.value:
            return self._handle_handshake(payload)
        if kind == MessageKind.STATE_REQUEST.value:
            return make_message(
                MessageKind.STATE_RESPONSE,
                self.config.cluster_name,
                {"snapshot": self._store.create_snapshot()},
            )
        if kind == MessageKind.OPERATION.value:
            self._handle_remote_operation(payload)
            return make_message(
                MessageKind.OPERATION,
                self.config.cluster_name,
                {"status": "ok"},
            )
        return make_message(
            MessageKind.ERROR,
            self.config.cluster_name,
            {"reason": f"unsupported message kind: {kind!r}"},
        )

    def _handle_handshake(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Register the peer from a handshake request and return known members.
        """
        raw_address = payload.get("address")
        if isinstance(raw_address, dict):
            try:
                self._add_member(NodeAddress.from_dict(raw_address))
            except (KeyError, TypeError, ValueError):
                pass
        return make_message(
            MessageKind.HANDSHAKE_ACK,
            self.config.cluster_name,
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
            return
        self._store.apply_mutation(payload)

    def _dispatch_topic(self, topic_name: str, message: Any) -> None:
        """
        Invoke all subscriber callbacks for one topic message.
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
        Register a topic callback and return subscription ID.
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
        """Publish a topic message locally and replicate to peers."""
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
        value = self._store.map_get(map_name, key, default)
        return value

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
        payload = {"index": index}
        if index is None:
            payload = {"index": None}
        return self._submit_local_operation(
            collection="list",
            name=list_name,
            action="pop",
            values=payload,
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
