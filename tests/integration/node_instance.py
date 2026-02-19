"""
Standalone node process used by integration tests.

The script starts one :class:`distributed_collections.cluster.ClusterNode`,
prints a single JSON ``ready`` event to stdout, and then accepts line-delimited
JSON commands through stdin.

Why this exists
---------------
Integration tests need realistic process boundaries. Running two independent
Python interpreter instances mirrors production deployment better than calling
the library from one process with multiple in-memory objects.

Protocol
--------
Input command shape::

    {"cmd": "<name>", "...": "..."}

Output response shape::

    {"ok": true, "result": ...}
    {"ok": false, "error": "..."}
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Ensure local package imports work when this script is launched via subprocess.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from distributed_collections import (  # noqa: E402
    ClusterConfig,
    ClusterNode,
    ConsistencyConfig,
    ConsistencyMode,
    DiscoveryMode,
    NodeAddress,
    ObservabilityConfig,
    SecurityConfig,
    StaticDiscoveryConfig,
    create_node,
)


def parse_seed_addresses(raw: str) -> list[NodeAddress]:
    """
    Parse a comma-separated ``host:port`` seed list into addresses.

    Parameters
    ----------
    raw:
        Comma-separated list such as ``"127.0.0.1:5701,127.0.0.1:5702"``.
    """
    if not raw.strip():
        return []
    seeds: list[NodeAddress] = []
    for token in raw.split(","):
        item = token.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"Invalid seed format {item!r}; expected host:port.")
        host, raw_port = item.rsplit(":", 1)
        seeds.append(NodeAddress(host=host.strip(), port=int(raw_port)))
    return seeds


def emit(payload: dict[str, Any]) -> None:
    """Emit one JSON line to stdout and flush immediately."""
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)


def build_parser() -> argparse.ArgumentParser:
    """Create and configure CLI argument parser for node process startup."""
    parser = argparse.ArgumentParser(description="Integration helper node process")
    parser.add_argument("--cluster", required=True, help="Cluster logical name")
    parser.add_argument("--host", default="127.0.0.1", help="Bind/advertise host")
    parser.add_argument("--port", type=int, required=True, help="Bind/advertise port")
    parser.add_argument(
        "--seeds",
        default="",
        help="Comma-separated host:port static seed peers",
    )
    parser.add_argument(
        "--discovery",
        default="static",
        choices=("static", "multicast", "both"),
        help="Discovery strategy for this process",
    )
    parser.add_argument(
        "--shared-token",
        default="",
        help="Optional shared token for HMAC message authentication",
    )
    parser.add_argument(
        "--consistency",
        default="linearizable",
        choices=("best_effort", "quorum", "all", "linearizable"),
        help="Write consistency mode",
    )
    parser.add_argument(
        "--observability-port",
        type=int,
        default=0,
        help="Optional HTTP observability endpoint port; disabled when 0",
    )
    parser.add_argument(
        "--store-backend",
        default="memory",
        choices=("memory", "redis"),
        help="Collection store backend to use",
    )
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:6379/0",
        help="Redis URL used when --store-backend=redis",
    )
    parser.add_argument(
        "--redis-namespace",
        default="distributed-collections",
        help="Redis key namespace used when --store-backend=redis",
    )
    return parser


def selected_discovery_modes(name: str) -> tuple[DiscoveryMode, ...]:
    """Translate CLI discovery option into ``DiscoveryMode`` tuple."""
    if name == "static":
        return (DiscoveryMode.STATIC,)
    if name == "multicast":
        return (DiscoveryMode.MULTICAST,)
    return (DiscoveryMode.STATIC, DiscoveryMode.MULTICAST)


def handle_command(
    *,
    node: ClusterNode,
    command: dict[str, Any],
    topic_buffers: dict[str, list[Any]],
    topic_subscriptions: dict[str, str],
) -> tuple[bool, Any]:
    """
    Execute one JSON command against the running cluster node.

    Returns
    -------
    tuple[bool, Any]
        Pair of ``(ok, result_or_error_message)``.
    """
    cmd = str(command.get("cmd", "")).strip()
    if not cmd:
        return False, "Missing command name in 'cmd' field."

    if cmd == "ping":
        return True, "pong"
    if cmd == "join":
        peers = node.join_cluster()
        return True, [peer.as_dict() for peer in peers]
    if cmd == "members":
        return True, [member.as_dict() for member in node.members()]
    if cmd == "stats":
        return True, node.stats()
    if cmd == "health":
        return True, node.health()
    if cmd == "traces":
        return True, node.recent_traces()

    if cmd == "map_put":
        target = node.get_map(str(command["name"]))
        return True, target.put(str(command["key"]), command.get("value"))
    if cmd == "map_get":
        target = node.get_map(str(command["name"]))
        default = command["default"] if "default" in command else None
        return True, target.get(str(command["key"]), default)
    if cmd == "map_items":
        target = node.get_map(str(command["name"]))
        return True, target.items_dict()

    if cmd == "list_append":
        target = node.get_list(str(command["name"]))
        target.append(command.get("value"))
        return True, None
    if cmd == "list_values":
        target = node.get_list(str(command["name"]))
        return True, target.values()

    if cmd == "queue_offer":
        target = node.get_queue(str(command["name"]))
        target.offer(command.get("value"))
        return True, None
    if cmd == "queue_poll":
        target = node.get_queue(str(command["name"]))
        return True, target.poll()
    if cmd == "queue_values":
        target = node.get_queue(str(command["name"]))
        return True, target.values()

    if cmd == "topic_subscribe":
        topic_name = str(command["name"])
        topic = node.get_topic(topic_name)
        bucket = topic_buffers.setdefault(topic_name, [])
        if topic_name not in topic_subscriptions:
            subscription_id = topic.subscribe(lambda message, store=bucket: store.append(message))
            topic_subscriptions[topic_name] = subscription_id
        return True, topic_subscriptions[topic_name]
    if cmd == "topic_publish":
        topic = node.get_topic(str(command["name"]))
        topic.publish(command.get("message"))
        return True, None
    if cmd == "topic_messages":
        topic_name = str(command["name"])
        return True, list(topic_buffers.get(topic_name, []))

    if cmd == "stop":
        return True, "__stop__"
    return False, f"Unknown command {cmd!r}"


def run() -> int:
    """Run helper process lifecycle and command loop."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        seeds = parse_seed_addresses(args.seeds)
        config = ClusterConfig(
            cluster_name=args.cluster,
            bind=NodeAddress(args.host, args.port),
            advertise_host=args.host,
            static_discovery=StaticDiscoveryConfig(seeds=seeds),
            enabled_discovery=selected_discovery_modes(args.discovery),
            security=SecurityConfig(shared_token=args.shared_token or None),
            consistency=ConsistencyConfig(mode=ConsistencyMode(args.consistency)),
            observability=ObservabilityConfig(
                enable_http=args.observability_port > 0,
                host=args.host,
                port=args.observability_port if args.observability_port > 0 else 8085,
            ),
        )
        backend_options: dict[str, Any] = {}
        if args.store_backend == "redis":
            backend_options = {
                "redis_url": args.redis_url,
                "namespace": args.redis_namespace,
            }
        node = create_node(
            config,
            backend=args.store_backend,
            **backend_options,
        )
        node.start(join=True)
    except Exception as exc:  # noqa: BLE001 - entrypoint should return clear startup error
        emit({"ok": False, "error": f"startup failed: {exc}"})
        return 1

    topic_buffers: dict[str, list[Any]] = {}
    topic_subscriptions: dict[str, str] = {}
    emit({"event": "ready", "port": args.port})

    try:
        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                command = json.loads(line)
                if not isinstance(command, dict):
                    raise ValueError("Command must be a JSON object.")
                ok, result = handle_command(
                    node=node,
                    command=command,
                    topic_buffers=topic_buffers,
                    topic_subscriptions=topic_subscriptions,
                )
                if ok and result == "__stop__":
                    emit({"ok": True, "result": None})
                    break
                if ok:
                    emit({"ok": True, "result": result})
                else:
                    emit({"ok": False, "error": result})
            except Exception as exc:  # noqa: BLE001 - command loop must stay alive for tests
                emit({"ok": False, "error": str(exc)})
    finally:
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
