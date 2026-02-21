"""
Minimal FastAPI example for distributed-python-collections.

Run two app instances with the same DPC_CLUSTER_NAME and different DPC_BIND_PORT
values to see replication on the "demo" map.
"""

from __future__ import annotations

import os
from typing import Any

import uvicorn
from distributed_collections import (
    ClusterConfig,
    ClusterNode,
    NodeAddress,
    PersistenceConfig,
    WriteAheadLogConfig,
)
from fastapi import FastAPI, HTTPException

app = FastAPI(title="dpc minimal fastapi example", version="0.1.0")
_node: ClusterNode | None = None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    return int(raw) if raw else default


def _require_node() -> ClusterNode:
    if _node is None:
        raise HTTPException(status_code=503, detail="Cluster node not started.")
    return _node


@app.on_event("startup")
def on_startup() -> None:
    global _node
    if _node is not None:
        return
    config = ClusterConfig(
        cluster_name=os.getenv("DPC_CLUSTER_NAME", "dpc-fastapi-example"),
        bind=NodeAddress(os.getenv("DPC_BIND_HOST", "0.0.0.0"), _env_int("DPC_BIND_PORT", 5711)),
        # Keep demo runs ephemeral (memory only).
        persistence=PersistenceConfig(enabled=False),
        wal=WriteAheadLogConfig(enabled=False),
    )
    _node = ClusterNode(config)
    _node.start(join=True)


@app.on_event("shutdown")
def on_shutdown() -> None:
    global _node
    if _node is None:
        return
    try:
        _node.stop()
    finally:
        _node = None


@app.get("/")
def root() -> dict[str, Any]:
    node = _require_node()
    return {
        "cluster": node.config.cluster_name,
        "node_id": node.node_id,
        "is_leader": node.is_leader,
        "members": [member.as_dict() for member in node.members()],
    }


@app.put("/map/{key}")
def map_put(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "value" not in payload:
        raise HTTPException(status_code=400, detail="Payload must include 'value'.")
    distributed_map = _require_node().get_map("demo")
    previous = distributed_map.put(key, payload["value"])
    return {"previous": previous, "value": distributed_map.get(key)}


@app.get("/map/{key}")
def map_get(key: str) -> dict[str, Any]:
    distributed_map = _require_node().get_map("demo")
    return {"value": distributed_map.get(key)}


@app.get("/map")
def map_items() -> dict[str, Any]:
    distributed_map = _require_node().get_map("demo")
    return {"items": distributed_map.items_dict()}


def main(port: int | None = None) -> int:
    host = os.getenv("DPC_API_HOST", "0.0.0.0")
    api_port = int(port) if port is not None else _env_int("DPC_API_PORT", 8000)
    uvicorn.run("dpc_example.fastapi_app:app", host=host, port=api_port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
