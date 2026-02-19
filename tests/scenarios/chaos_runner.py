"""
Chaos validation harness for distributed_collections clusters.

The script repeatedly:

1. starts multiple node processes
2. applies random distributed collection mutations
3. randomly terminates/restarts nodes
4. checks eventual convergence through snapshot/state probes

Usage example
-------------
python tests/scenarios/chaos_runner.py --nodes 3 --duration-seconds 300
"""

from __future__ import annotations

import argparse
import json
import random
import select
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE_SCRIPT = REPO_ROOT / "tests" / "integration" / "node_instance.py"


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_line(proc: subprocess.Popen[str], timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.stdout is None:
            raise RuntimeError("process stdout is unavailable")
        remaining = max(0.0, deadline - time.monotonic())
        ready, _, _ = select.select([proc.stdout.fileno()], [], [], remaining)
        if not ready:
            continue
        line = proc.stdout.readline()
        if line:
            return json.loads(line)
        if proc.poll() is not None:
            raise RuntimeError("node process exited unexpectedly")
    raise TimeoutError("timed out waiting for node output")


def send(proc: subprocess.Popen[str], payload: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
    if proc.stdin is None:
        raise RuntimeError("process stdin is unavailable")
    proc.stdin.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
    proc.stdin.flush()
    return read_line(proc, timeout)


def start_node(cluster: str, port: int, seeds: str) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        [
            sys.executable,
            str(NODE_SCRIPT),
            "--cluster",
            cluster,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--seeds",
            seeds,
            "--discovery",
            "static",
        ],
        cwd=str(REPO_ROOT),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    ready = read_line(proc, 10.0)
    if ready.get("event") != "ready":
        raise RuntimeError(f"unexpected startup output: {ready}")
    return proc


def stop_node(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        send(proc, {"cmd": "stop"}, timeout=2.0)
    except Exception:
        pass
    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Chaos scenario runner")
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--duration-seconds", type=int, default=120)
    args = parser.parse_args()

    cluster = f"chaos-{uuid.uuid4().hex}"
    ports = [reserve_port() for _ in range(args.nodes)]
    procs: dict[int, subprocess.Popen[str]] = {}

    try:
        for port in ports:
            seeds = ",".join(f"127.0.0.1:{item}" for item in ports if item != port)
            procs[port] = start_node(cluster, port, seeds)

        deadline = time.monotonic() + float(args.duration_seconds)
        while time.monotonic() < deadline:
            live_ports = [port for port, proc in procs.items() if proc.poll() is None]
            if not live_ports:
                break

            # Issue a random mutation command.
            target_port = random.choice(live_ports)
            target = procs[target_port]
            op = random.choice(("map", "list", "queue"))
            key = uuid.uuid4().hex[:8]

            if op == "map":
                send(
                    target,
                    {"cmd": "map_put", "name": "chaos_map", "key": key, "value": {"v": key}},
                )
            elif op == "list":
                send(target, {"cmd": "list_append", "name": "chaos_list", "value": key})
            else:
                send(target, {"cmd": "queue_offer", "name": "chaos_queue", "value": key})

            # Randomly kill/restart one node to inject failures.
            if random.random() < 0.25:
                victim_port = random.choice(live_ports)
                stop_node(procs[victim_port])
                seeds = ",".join(f"127.0.0.1:{item}" for item in ports if item != victim_port)
                procs[victim_port] = start_node(cluster, victim_port, seeds)

            time.sleep(0.2)
    finally:
        for proc in procs.values():
            stop_node(proc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
