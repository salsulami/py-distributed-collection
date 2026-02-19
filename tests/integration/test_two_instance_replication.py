"""
Two-instance integration test for distributed collection replication.

The test launches two independent node scripts, drives operations through stdin
commands, and validates eventual consistency across process boundaries.
"""

from __future__ import annotations

import json
import select
import socket
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE_SCRIPT = Path(__file__).with_name("node_instance.py")


def reserve_local_port() -> int:
    """
    Reserve and release one ephemeral localhost port.

    Returns
    -------
    int
        Port number that is very likely to be free for immediate use.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_json_line(proc: subprocess.Popen[str], timeout_seconds: float) -> dict[str, Any]:
    """
    Read one JSON line from a subprocess stdout pipe with timeout.

    Raises
    ------
    TimeoutError
        If no line is received before the timeout.
    RuntimeError
        If the process exits before producing output.
    """
    if proc.stdout is None:
        raise RuntimeError("Process stdout pipe is not available.")
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        ready, _, _ = select.select([proc.stdout.fileno()], [], [], remaining)
        if not ready:
            continue
        line = proc.stdout.readline()
        if line == "":
            stderr = ""
            if proc.stderr is not None:
                stderr = proc.stderr.read()
            raise RuntimeError(f"Process exited before response. stderr={stderr!r}")
        return json.loads(line)
    raise TimeoutError("Timed out waiting for subprocess output.")


def start_node_process(*, cluster: str, host: str, port: int, seeds: str) -> subprocess.Popen[str]:
    """
    Start one integration node helper process and wait for ready event.
    """
    command = [
        sys.executable,
        str(NODE_SCRIPT),
        "--cluster",
        cluster,
        "--host",
        host,
        "--port",
        str(port),
        "--seeds",
        seeds,
        "--discovery",
        "static",
    ]
    proc = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    ready = read_json_line(proc, timeout_seconds=10.0)
    if ready.get("event") != "ready":
        raise RuntimeError(f"Unexpected startup response: {ready}")
    return proc


def send_command(
    proc: subprocess.Popen[str],
    payload: dict[str, Any],
    *,
    timeout_seconds: float = 5.0,
) -> Any:
    """
    Send one command to node helper and return response result.
    """
    if proc.stdin is None:
        raise RuntimeError("Process stdin pipe is not available.")
    proc.stdin.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
    proc.stdin.flush()
    response = read_json_line(proc, timeout_seconds=timeout_seconds)
    if not response.get("ok", False):
        raise AssertionError(f"Node command failed: {response.get('error')}")
    return response.get("result")


def send_eventually(
    proc: subprocess.Popen[str],
    payload: dict[str, Any],
    *,
    timeout_seconds: float = 8.0,
    interval_seconds: float = 0.2,
) -> Any:
    """
    Retry a command until it succeeds or timeout is reached.
    """
    deadline = time.monotonic() + timeout_seconds
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return send_command(proc, payload, timeout_seconds=interval_seconds + 1.0)
        except Exception as exc:  # noqa: BLE001 - preserve final error context
            last_exc = exc
            time.sleep(interval_seconds)
    if last_exc is not None:
        raise AssertionError(f"Command did not succeed in time: {payload}") from last_exc
    raise AssertionError(f"Command did not succeed in time: {payload}")


def stop_node_process(proc: subprocess.Popen[str]) -> None:
    """
    Stop helper process gracefully and force kill only as a fallback.
    """
    if proc.poll() is not None:
        return
    try:
        send_command(proc, {"cmd": "stop"}, timeout_seconds=3.0)
        proc.wait(timeout=5.0)
    except Exception:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3.0)


def assert_eventually(predicate, *, timeout_seconds: float, interval_seconds: float, message: str) -> None:
    """
    Repeatedly run ``predicate`` until it returns true or timeout occurs.
    """
    deadline = time.monotonic() + timeout_seconds
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001 - retained as context for failure
            last_exc = exc
        time.sleep(interval_seconds)
    if last_exc is not None:
        raise AssertionError(message) from last_exc
    raise AssertionError(message)


class TwoInstanceReplicationIntegrationTest(unittest.TestCase):
    """
    Validates replication semantics across two independent node processes.
    """

    def test_replication_for_map_list_queue_and_topic(self) -> None:
        """
        Ensure map/list/queue/topic operations replicate between two instances.
        """
        cluster_name = f"it-{uuid.uuid4().hex}"
        host = "127.0.0.1"
        port_1 = reserve_local_port()
        port_2 = reserve_local_port()

        node_1 = start_node_process(
            cluster=cluster_name,
            host=host,
            port=port_1,
            seeds=f"{host}:{port_2}",
        )
        node_2 = start_node_process(
            cluster=cluster_name,
            host=host,
            port=port_2,
            seeds=f"{host}:{port_1}",
        )
        self.addCleanup(stop_node_process, node_1)
        self.addCleanup(stop_node_process, node_2)

        # Force a deterministic join pass on both nodes once both are online.
        send_eventually(node_1, {"cmd": "join"})
        send_eventually(node_2, {"cmd": "join"})

        send_eventually(
            node_1,
            {
                "cmd": "map_put",
                "name": "users",
                "key": "u-1",
                "value": {"name": "Alice", "tier": "gold"},
            },
        )
        assert_eventually(
            lambda: send_command(
                node_2,
                {"cmd": "map_get", "name": "users", "key": "u-1"},
            )
            == {"name": "Alice", "tier": "gold"},
            timeout_seconds=8.0,
            interval_seconds=0.2,
            message="Map replication from node_1 to node_2 did not converge.",
        )

        send_eventually(
            node_2,
            {"cmd": "list_append", "name": "events", "value": {"type": "order-created"}},
        )
        assert_eventually(
            lambda: send_command(node_1, {"cmd": "list_values", "name": "events"})
            == [{"type": "order-created"}],
            timeout_seconds=8.0,
            interval_seconds=0.2,
            message="List replication from node_2 to node_1 did not converge.",
        )

        send_eventually(node_1, {"cmd": "queue_offer", "name": "jobs", "value": "job-1"})
        assert_eventually(
            lambda: send_command(node_2, {"cmd": "queue_values", "name": "jobs"}) == ["job-1"],
            timeout_seconds=8.0,
            interval_seconds=0.2,
            message="Queue replication from node_1 to node_2 did not converge.",
        )
        polled = send_command(node_2, {"cmd": "queue_poll", "name": "jobs"})
        self.assertEqual("job-1", polled)
        assert_eventually(
            lambda: send_command(node_1, {"cmd": "queue_values", "name": "jobs"}) == [],
            timeout_seconds=8.0,
            interval_seconds=0.2,
            message="Queue poll mutation did not replicate back to node_1.",
        )

        send_eventually(node_2, {"cmd": "topic_subscribe", "name": "alerts"})
        send_eventually(
            node_1,
            {
                "cmd": "topic_publish",
                "name": "alerts",
                "message": {"level": "info", "text": "cluster-sync"},
            },
        )
        assert_eventually(
            lambda: send_command(node_2, {"cmd": "topic_messages", "name": "alerts"})
            == [{"level": "info", "text": "cluster-sync"}],
            timeout_seconds=8.0,
            interval_seconds=0.2,
            message="Topic publication from node_1 was not received on node_2.",
        )


if __name__ == "__main__":
    unittest.main()
