"""
Soak-test harness for long-duration replication stability.

Usage example
-------------
python tests/scenarios/soak_runner.py --duration-seconds 3600 --interval-ms 50
"""

from __future__ import annotations

import argparse
import json
import select
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE_SCRIPT = REPO_ROOT / "tests" / "integration" / "node_instance.py"


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_json(proc: subprocess.Popen[str], timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.stdout is None:
            raise RuntimeError("stdout unavailable")
        remaining = max(0.0, deadline - time.monotonic())
        ready, _, _ = select.select([proc.stdout.fileno()], [], [], remaining)
        if not ready:
            continue
        line = proc.stdout.readline()
        if line:
            return json.loads(line)
        if proc.poll() is not None:
            raise RuntimeError("node process exited")
    raise TimeoutError("node response timeout")


def send(proc: subprocess.Popen[str], cmd: dict, timeout: float = 5.0) -> dict:
    if proc.stdin is None:
        raise RuntimeError("stdin unavailable")
    proc.stdin.write(json.dumps(cmd, separators=(",", ":"), sort_keys=True) + "\n")
    proc.stdin.flush()
    return read_json(proc, timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Soak scenario runner")
    parser.add_argument("--duration-seconds", type=int, default=600)
    parser.add_argument("--interval-ms", type=int, default=100)
    args = parser.parse_args()

    cluster = f"soak-{uuid.uuid4().hex}"
    port_1 = reserve_port()
    port_2 = reserve_port()

    proc_1 = subprocess.Popen(
        [
            sys.executable,
            str(NODE_SCRIPT),
            "--cluster",
            cluster,
            "--host",
            "127.0.0.1",
            "--port",
            str(port_1),
            "--seeds",
            f"127.0.0.1:{port_2}",
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
    proc_2 = subprocess.Popen(
        [
            sys.executable,
            str(NODE_SCRIPT),
            "--cluster",
            cluster,
            "--host",
            "127.0.0.1",
            "--port",
            str(port_2),
            "--seeds",
            f"127.0.0.1:{port_1}",
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

    try:
        read_json(proc_1, 10.0)
        read_json(proc_2, 10.0)
        send(proc_1, {"cmd": "join"})
        send(proc_2, {"cmd": "join"})

        deadline = time.monotonic() + float(args.duration_seconds)
        counter = 0
        while time.monotonic() < deadline:
            counter += 1
            send(
                proc_1,
                {"cmd": "map_put", "name": "soak", "key": f"k-{counter}", "value": counter},
            )
            send(proc_2, {"cmd": "list_append", "name": "soak_events", "value": counter})
            send(proc_1, {"cmd": "queue_offer", "name": "soak_queue", "value": counter})
            # Pull stats periodically to watch sustained replication behavior.
            send(proc_1, {"cmd": "stats"})
            send(proc_2, {"cmd": "stats"})
            time.sleep(float(args.interval_ms) / 1000.0)
    finally:
        for proc in (proc_1, proc_2):
            if proc.poll() is None:
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
