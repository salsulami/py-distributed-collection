"""
Snapshot persistence utilities for cluster state recovery.

Persistence is intentionally simple and robust:

* snapshots are serialized as JSON objects
* writes are atomic via ``os.replace`` of a temporary file
* optional ``fsync`` is available for stronger durability semantics
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class SnapshotPersistence:
    """
    Manage full-state snapshot reads and atomic writes.

    Parameters
    ----------
    snapshot_path:
        Destination file path for persisted snapshots.
    fsync:
        If true, force the data and directory entries to disk.
    """

    def __init__(self, snapshot_path: str, *, fsync: bool = True) -> None:
        self._path = Path(snapshot_path).expanduser().resolve()
        self._fsync = bool(fsync)

    @property
    def path(self) -> Path:
        """Return fully resolved snapshot path."""
        return self._path

    def load(self) -> dict[str, Any] | None:
        """
        Load persisted snapshot from disk.

        Returns
        -------
        dict[str, Any] | None
            Decoded snapshot object, or ``None`` when file is absent.
        """
        if not self._path.exists():
            return None
        with self._path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError("Snapshot file must contain a JSON object.")
        return data

    def save(self, snapshot: dict[str, Any]) -> None:
        """
        Persist snapshot atomically to disk.

        The method writes to ``<snapshot>.tmp`` and then renames to guarantee
        readers only observe either the old file or the complete new file.
        """
        parent = self._path.parent
        parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")

        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, separators=(",", ":"), sort_keys=True)
            handle.flush()
            if self._fsync:
                os.fsync(handle.fileno())
        os.replace(temp_path, self._path)

        if self._fsync:
            dir_fd = os.open(str(parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
