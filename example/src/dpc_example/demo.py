"""
Comprehensive runnable demo for distributed collections.

The demo starts two cluster nodes and showcases:

* distributed map (hash map)
* distributed list
* distributed queue
* distributed topic

Run after installing this example package:

    dpc-example
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any

from distributed_collections import (
    ClusterConfig,
    ConsistencyConfig,
    ConsistencyMode,
    DiscoveryMode,
    NodeAddress,
    PersistenceConfig,
    StaticDiscoveryConfig,
    WriteAheadLogConfig,
    create_node,
)
from distributed_collections.exceptions import BackendNotAvailableError


def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval_seconds: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_seconds)
    return False


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="distributed-python-collections example")
    parser.add_argument("--backend", choices=("memory", "redis"), default="memory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port-a", type=int, default=5711)
    parser.add_argument("--port-b", type=int, default=5712)
    parser.add_argument("--cluster-name", default="example-cluster")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--redis-namespace", default=f"dpc-example:{uuid.uuid4().hex[:8]}")
    parser.add_argument(
        "--consistency",
        choices=("best_effort", "quorum", "all", "linearizable"),
        default="best_effort",
    )
    return parser


def _node_config(
    *,
    cluster_name: str,
    host: str,
    port: int,
    peer_port: int,
    consistency_mode: str,
) -> ClusterConfig:
    return ClusterConfig(
        cluster_name=cluster_name,
        bind=NodeAddress(host, port),
        advertise_host=host,
        enabled_discovery=(DiscoveryMode.STATIC,),
        static_discovery=StaticDiscoveryConfig(seeds=[NodeAddress(host, peer_port)]),
        consistency=ConsistencyConfig(mode=ConsistencyMode(consistency_mode)),
        # Keep the demo ephemeral across process restarts.
        persistence=PersistenceConfig(enabled=False),
        wal=WriteAheadLogConfig(enabled=False),
    )


def _node_backend_options(args: argparse.Namespace) -> dict[str, Any]:
    if args.backend == "redis":
        return {
            "redis_url": args.redis_url,
            "namespace": args.redis_namespace,
        }
    return {}


def _print_step(title: str, payload: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, sort_keys=True))


def run_demo(args: argparse.Namespace) -> int:
    backend_options = _node_backend_options(args)
    node_a = create_node(
        _node_config(
            cluster_name=args.cluster_name,
            host=args.host,
            port=args.port_a,
            peer_port=args.port_b,
            consistency_mode=args.consistency,
        ),
        backend=args.backend,
        **backend_options,
    )
    node_b = create_node(
        _node_config(
            cluster_name=args.cluster_name,
            host=args.host,
            port=args.port_b,
            peer_port=args.port_a,
            consistency_mode=args.consistency,
        ),
        backend=args.backend,
        **backend_options,
    )

    topic_messages: list[Any] = []
    try:
        node_a.start(join=False)
        node_b.start(join=False)
        node_a.join_cluster()
        node_b.join_cluster()

        _print_step(
            "Cluster Members",
            {
                "node_a_members": [item.as_dict() for item in node_a.members()],
                "node_b_members": [item.as_dict() for item in node_b.members()],
                "backend": args.backend,
            },
        )

        # Map / hash map demo.
        map_a = node_a.get_map("users")
        map_b = node_b.get_map("users")
        map_a.put("u-1", {"name": "Alice", "role": "admin"})
        _wait_until(lambda: map_b.get("u-1") is not None)
        _print_step("Distributed Map", {"node_a": map_a.items_dict(), "node_b": map_b.items_dict()})

        # List demo.
        list_a = node_a.get_list("events")
        list_b = node_b.get_list("events")
        list_a.append({"kind": "user-created", "id": "u-1"})
        list_a.extend([{"kind": "user-verified", "id": "u-1"}])
        _wait_until(lambda: len(list_b.values()) >= 2)
        _print_step("Distributed List", {"node_a": list_a.values(), "node_b": list_b.values()})

        # Queue demo.
        queue_a = node_a.get_queue("jobs")
        queue_b = node_b.get_queue("jobs")
        queue_a.offer("job-1")
        queue_a.offer("job-2")
        _wait_until(lambda: queue_b.size() >= 2)
        first_job = queue_b.poll()
        _wait_until(lambda: queue_a.size() == 1)
        _print_step(
            "Distributed Queue",
            {
                "polled_from_node_b": first_job,
                "node_a_remaining": queue_a.values(),
                "node_b_remaining": queue_b.values(),
            },
        )

        # Topic demo.
        topic_a = node_a.get_topic("alerts")
        topic_b = node_b.get_topic("alerts")
        _subscription = topic_b.subscribe(lambda message: topic_messages.append(message))
        topic_a.publish({"level": "info", "message": "topic broadcast"})
        _wait_until(lambda: len(topic_messages) >= 1)
        _print_step("Distributed Topic", {"received_on_node_b": topic_messages})

        _print_step("Node A Stats", node_a.stats())
        _print_step("Node B Stats", node_b.stats())
        return 0
    finally:
        node_a.stop()
        node_b.stop()


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        return run_demo(args)
    except BackendNotAvailableError as exc:
        print(f"Redis backend not available: {exc}", file=sys.stderr)
        print(
            "Install optional plugin first: pip install distributed-python-collections-redis",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
