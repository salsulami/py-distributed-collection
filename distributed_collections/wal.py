"""
Write-ahead log utilities for durable operation journaling.

The WAL stores one JSON operation per line in append order. On node startup,
entries are replayed after snapshot load to restore committed mutations.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from threading import RLock
from typing import Any


class WriteAheadLog:
    """
    Append-only write-ahead log for ordered mutation entries.

    Parameters
    ----------
    wal_path:
        File path to append log entries to.
    fsync_each_write:
        If true, ``fsync`` after every append for stronger durability.
    """

    def __init__(self, wal_path: str, *, fsync_each_write: bool = False) -> None:
        self._path = Path(wal_path).expanduser().resolve()
        self._fsync_each_write = bool(fsync_each_write)
        self._lock = RLock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        """Return resolved WAL file path."""
        return self._path

    def append(self, entry: dict[str, Any]) -> None:
        """
        Append one entry to WAL as JSON line.
        """
        line = json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n"
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                if self._fsync_each_write:
                    os.fsync(handle.fileno())

    def replay(self) -> list[dict[str, Any]]:
        """
        Replay all parseable entries from WAL file.

        Corrupt/truncated lines are skipped to maximize recovery progress.
        """
        if not self._path.exists():
            return []
        entries: list[dict[str, Any]] = []
        with self._lock:
            with self._path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(parsed, dict):
                        entries.append(parsed)
        return entries

    def truncate(self) -> None:
        """
        Truncate WAL file after successful snapshot checkpoint.
        """
        with self._lock:
            with self._path.open("w", encoding="utf-8"):
                return
