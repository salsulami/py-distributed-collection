"""
FastAPI application showcasing distributed collection replication across nodes.

Run multiple instances of this app with the same cluster name and discovery
settings, then write to one instance and read from another.
"""

from __future__ import annotations
import argparse
import logging
import os
import socket
import threading
from collections import deque
from typing import Any

import uvicorn
from distributed_collections import (
    ClusterConfig,
    ClusterNode,
    ConsistencyConfig,
    ConsistencyMode,
    DiscoveryMode,
    NodeAddress,
    StaticDiscoveryConfig,
    create_node,
)
from fastapi import FastAPI, HTTPException

app = FastAPI(title="distributed-python-collections FastAPI example", version="0.1.0")
_LOGGER = logging.getLogger(__name__)

_topic_lock = threading.RLock()
_topic_subscription_ids: dict[str, str] = {}
_topic_messages: dict[str, deque[Any]] = {}
_TOPIC_HISTORY_SIZE = 200

_node: ClusterNode | None = None
_PORT_SCAN_LIMIT = 200


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def _get_env_optional(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value if value else None


def _parse_int(name: str, default: int) -> int:
    raw = _get_env(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer.") from exc


def _find_first_available_port(host: str, preferred_port: int, *, scan_limit: int = _PORT_SCAN_LIMIT) -> int:
    if preferred_port <= 0:
        raise RuntimeError("Port value must be >= 1.")
    for offset in range(max(1, int(scan_limit))):
        candidate = preferred_port + offset
        if candidate > 65535:
            break
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, candidate))
            except OSError:
                continue
        return candidate
    raise RuntimeError(
        f"Could not find an available TCP port from {preferred_port} "
        f"within {scan_limit} attempts on host {host!r}."
    )


def _parse_seeds(raw_seeds: str) -> list[NodeAddress]:
    seeds: list[NodeAddress] = []
    if not raw_seeds.strip():
        return seeds
    for item in raw_seeds.split(","):
        entry = item.strip()
        if not entry:
            continue
        if ":" not in entry:
            raise RuntimeError(
                "DPC_STATIC_SEEDS entries must use host:port format "
                f"(invalid entry: {entry!r})."
            )
        host, raw_port = entry.rsplit(":", 1)
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise RuntimeError(
                f"DPC_STATIC_SEEDS port must be int (invalid entry: {entry!r})."
            ) from exc
        seeds.append(NodeAddress(host=host, port=port))
    return seeds


def _build_cluster_config() -> ClusterConfig:
    bind_host = _get_env("DPC_BIND_HOST", "0.0.0.0")
    bind_port_env = _get_env_optional("DPC_BIND_PORT")
    bind_port = _parse_int("DPC_BIND_PORT", 5711)
    cluster_name = _get_env("DPC_CLUSTER_NAME", "dpc-fastapi-example")
    advertise_host = _get_env_optional("DPC_ADVERTISE_HOST")
    consistency_name = _get_env("DPC_CONSISTENCY", "best_effort")
    discovery_name = _get_env("DPC_DISCOVERY", "multicast").lower()
    static_seeds = _parse_seeds(_get_env_optional("DPC_STATIC_SEEDS") or "")

    config_kwargs: dict[str, Any] = {
        "cluster_name": cluster_name,
        "bind": NodeAddress(bind_host, bind_port),
        "consistency": ConsistencyConfig(mode=ConsistencyMode(consistency_name)),
    }
    if advertise_host:
        config_kwargs["advertise_host"] = advertise_host

    if bind_port_env is None:
        chosen_bind_port = _find_first_available_port(bind_host, bind_port)
        if chosen_bind_port != bind_port:
            _LOGGER.warning(
                "Default cluster bind port %s is in use; selected %s instead.",
                bind_port,
                chosen_bind_port,
            )
        bind_port = chosen_bind_port
        config_kwargs["bind"] = NodeAddress(bind_host, bind_port)

    if discovery_name == "static":
        if not static_seeds:
            raise RuntimeError("DPC_DISCOVERY=static requires DPC_STATIC_SEEDS.")
        config_kwargs["enabled_discovery"] = (DiscoveryMode.STATIC,)
        config_kwargs["static_discovery"] = StaticDiscoveryConfig(seeds=static_seeds)
    elif discovery_name == "both":
        config_kwargs["enabled_discovery"] = (DiscoveryMode.MULTICAST, DiscoveryMode.STATIC)
        config_kwargs["static_discovery"] = StaticDiscoveryConfig(seeds=static_seeds)
    elif discovery_name != "multicast":
        raise RuntimeError("DPC_DISCOVERY must be one of: multicast, static, both.")

    return ClusterConfig(**config_kwargs)


def _build_cluster_node() -> ClusterNode:
    config = _build_cluster_config()
    backend = _get_env("DPC_BACKEND", "memory").lower()
    if backend == "redis":
        redis_url = _get_env("DPC_REDIS_URL", "redis://127.0.0.1:6379/0")
        redis_namespace = _get_env("DPC_REDIS_NAMESPACE", "dpc-fastapi-example")
        return create_node(
            config,
            backend="redis",
            redis_url=redis_url,
            namespace=redis_namespace,
        )
    if backend != "memory":
        raise RuntimeError("DPC_BACKEND must be memory or redis.")
    return create_node(config, backend="memory")


def _require_node() -> ClusterNode:
    if _node is None:
        raise HTTPException(status_code=503, detail="Cluster node not started.")
    return _node


def _record_topic_message(topic_name: str, message: Any) -> None:
    with _topic_lock:
        history = _topic_messages.setdefault(topic_name, deque(maxlen=_TOPIC_HISTORY_SIZE))
        history.append(message)


def _ensure_topic_subscription(topic_name: str) -> None:
    node = _require_node()
    with _topic_lock:
        if topic_name in _topic_subscription_ids:
            return
    topic = node.get_topic(topic_name)
    subscription_id = topic.subscribe(lambda message: _record_topic_message(topic_name, message))
    with _topic_lock:
        _topic_subscription_ids[topic_name] = subscription_id
        _topic_messages.setdefault(topic_name, deque(maxlen=_TOPIC_HISTORY_SIZE))


@app.on_event("startup")
def on_startup() -> None:
    global _node
    if _node is not None:
        return
    _node = _build_cluster_node()
    _node.start(join=True)


@app.on_event("shutdown")
def on_shutdown() -> None:
    global _node
    node = _node
    if node is None:
        return
    try:
        node.stop()
    finally:
        _node = None


@app.get("/")
def root() -> dict[str, Any]:
    node = _require_node()
    return {
        "message": "FastAPI replication example is running.",
        "node_id": node.node_id,
        "cluster": node.config.cluster_name,
        "is_leader": node.is_leader,
    }


@app.get("/cluster")
def cluster_state() -> dict[str, Any]:
    node = _require_node()
    return {
        "cluster": node.config.cluster_name,
        "node_id": node.node_id,
        "bind": node.config.bind.as_dict(),
        "advertise": node.config.advertise_address.as_dict(),
        "enabled_discovery": [mode.value for mode in node.config.enabled_discovery],
        "static_seeds": [seed.as_dict() for seed in node.config.static_discovery.seeds],
        "is_leader": node.is_leader,
        "members": [member.as_dict() for member in node.members()],
    }


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return _require_node().health()


@app.get("/stats")
def stats() -> dict[str, Any]:
    return _require_node().stats()


@app.put("/map/{name}/{key}")
def map_put(name: str, key: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "value" not in payload:
        raise HTTPException(status_code=400, detail="Payload must include 'value'.")
    distributed_map = _require_node().get_map(name)
    previous = distributed_map.put(key, payload["value"])
    return {"previous": previous, "current": distributed_map.get(key)}


@app.get("/map/{name}")
def map_items(name: str) -> dict[str, Any]:
    distributed_map = _require_node().get_map(name)
    return {"items": distributed_map.items_dict()}


@app.get("/map/{name}/{key}")
def map_get(name: str, key: str) -> dict[str, Any]:
    distributed_map = _require_node().get_map(name)
    return {"value": distributed_map.get(key)}


@app.delete("/map/{name}/{key}")
def map_remove(name: str, key: str) -> dict[str, Any]:
    distributed_map = _require_node().get_map(name)
    removed = distributed_map.remove(key)
    return {"removed": removed}


@app.post("/list/{name}/append")
def list_append(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "value" not in payload:
        raise HTTPException(status_code=400, detail="Payload must include 'value'.")
    distributed_list = _require_node().get_list(name)
    distributed_list.append(payload["value"])
    return {"size": len(distributed_list)}


@app.post("/list/{name}/extend")
def list_extend(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("values")
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail="Payload must include list field 'values'.")
    distributed_list = _require_node().get_list(name)
    distributed_list.extend(values)
    return {"size": len(distributed_list)}


@app.post("/list/{name}/pop")
def list_pop(name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    distributed_list = _require_node().get_list(name)
    index = None if not payload else payload.get("index")
    value = distributed_list.pop(index)
    return {"value": value, "size": len(distributed_list)}


@app.get("/list/{name}")
def list_values(name: str) -> dict[str, Any]:
    distributed_list = _require_node().get_list(name)
    return {"values": distributed_list.values()}


@app.post("/queue/{name}/offer")
def queue_offer(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "value" not in payload:
        raise HTTPException(status_code=400, detail="Payload must include 'value'.")
    distributed_queue = _require_node().get_queue(name)
    distributed_queue.offer(payload["value"])
    return {"size": distributed_queue.size()}


@app.post("/queue/{name}/poll")
def queue_poll(name: str) -> dict[str, Any]:
    distributed_queue = _require_node().get_queue(name)
    value = distributed_queue.poll()
    return {"value": value, "size": distributed_queue.size()}


@app.get("/queue/{name}")
def queue_values(name: str) -> dict[str, Any]:
    distributed_queue = _require_node().get_queue(name)
    return {"values": distributed_queue.values()}


@app.post("/topic/{name}/publish")
def topic_publish(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "message" not in payload:
        raise HTTPException(status_code=400, detail="Payload must include 'message'.")
    _ensure_topic_subscription(name)
    topic = _require_node().get_topic(name)
    topic.publish(payload["message"])
    return {"status": "published"}


@app.get("/topic/{name}/messages")
def topic_messages(name: str) -> dict[str, Any]:
    _ensure_topic_subscription(name)
    with _topic_lock:
        values = list(_topic_messages.get(name, []))
    return {"messages": values}


def main(port:int) -> int:
    host = _get_env("DPC_API_HOST", "0.0.0.0")
    api_port_env = _get_env_optional("DPC_API_PORT")
    port = _parse_int("DPC_API_PORT", port)
    if api_port_env is None:
        chosen_api_port = _find_first_available_port(host, port)
        if chosen_api_port != port:
            _LOGGER.warning(
                "Default API port %s is in use; selected %s instead.",
                port,
                chosen_api_port,
            )
        port = chosen_api_port
    uvicorn.run("dpc_example.fastapi_app:app", host=host, port=port, reload=False)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Python script on a specific port.")
    parser.add_argument("--port", type=int, default=8000, help="The port number to use")
    args = parser.parse_args()
    raise SystemExit(main(args.port))
