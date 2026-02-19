"""
Load-test harness for throughput and queue/backpressure behavior.

Usage example
-------------
python tests/scenarios/load_runner.py --operations 50000 --workers 8
"""

from __future__ import annotations

import argparse
import json
import select
import socket
import subprocess
import sys
import threading
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
    raise TimeoutError("timed out waiting for node output")


def send(proc: subprocess.Popen[str], cmd: dict, timeout: float = 5.0) -> dict:
    if proc.stdin is None:
        raise RuntimeError("stdin unavailable")
    proc.stdin.write(json.dumps(cmd, separators=(",", ":"), sort_keys=True) + "\n")
    proc.stdin.flush()
    return read_json(proc, timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load scenario runner")
    parser.add_argument("--operations", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    cluster = f"load-{uuid.uuid4().hex}"
    port = reserve_port()

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
            "",
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
        read_json(proc, 10.0)
        errors: list[str] = []
        lock = threading.Lock()
        operations_per_worker = max(1, args.operations // max(1, args.workers))

        def run_worker(worker_id: int) -> None:
            for index in range(operations_per_worker):
                key = f"w{worker_id}-k{index}"
                try:
                    send(
                        proc,
                        {"cmd": "map_put", "name": "load_map", "key": key, "value": index},
                        timeout=5.0,
                    )
                except Exception as exc:  # noqa: BLE001 - capture worker failures
                    with lock:
                        errors.append(str(exc))
                    return

        threads = [
            threading.Thread(target=run_worker, args=(worker_id,), daemon=True)
            for worker_id in range(args.workers)
        ]
        start = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = max(time.perf_counter() - start, 0.001)
        stats = send(proc, {"cmd": "stats"})

        print(
            json.dumps(
                {
                    "operations": operations_per_worker * args.workers,
                    "elapsed_seconds": elapsed,
                    "ops_per_second": (operations_per_worker * args.workers) / elapsed,
                    "errors": len(errors),
                    "stats": stats.get("result"),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    finally:
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
