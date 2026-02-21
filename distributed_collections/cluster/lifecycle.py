from __future__ import annotations

import time

from ..config import DiscoveryMode
from ..discovery import MulticastDiscoveryResponder


class ClusterLifecycleMixin:
    """
    Node startup/shutdown lifecycle orchestration.
    """

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
            self._hydrate_all_component_ttls()

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
                    bind_host=self.config.bind.host,
                    group=self.config.multicast.group,
                    port=self.config.multicast.port,
                    socket_timeout_seconds=self.config.socket_timeout_seconds,
                    prefer_current_assigned_ip=bool(
                        getattr(self.config, "_auto_advertise_host", False)
                    ),
                )
                self._discovery_responder.start()

            self._running = True
            self._start_ttl_worker()
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
            self._stop_ttl_worker()

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
