from __future__ import annotations

import time
import uuid
from typing import Any

from ..exceptions import ClusterNotRunningError


class ClusterCPMixin:
    """
    CP subsystem internals (distributed lock state machine).
    """

    def _normalize_cp_lease_ms(self, lease_seconds: float | None) -> int:
        """Normalize requested lock lease to bounded milliseconds."""
        seconds = (
            self.config.cp.lock_default_lease_seconds
            if lease_seconds is None
            else float(lease_seconds)
        )
        if seconds <= 0:
            raise ValueError("Lock lease must be > 0 seconds.")
        seconds = min(seconds, self.config.cp.lock_max_lease_seconds)
        return max(1, int(seconds * 1000))

    def _expire_cp_lock_unlocked(self, lock_name: str, now_ms: int) -> None:
        """Drop lock state when lease has expired (caller must hold CP state lock)."""
        state = self._cp_locks.get(lock_name)
        if state is None:
            return
        if int(state.get("lease_deadline_ms", 0)) > now_ms:
            return
        self._cp_locks.pop(lock_name, None)
        self._inc_stat("cp_lock_expired")

    def _apply_cp_lock_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Apply one CP lock operation deterministically in replicated log order.
        """
        action = str(payload.get("action", ""))
        lock_name = str(payload.get("name", ""))
        owner_id = str(payload.get("owner_id", ""))
        now_ms = int(payload.get("now_ms", int(time.time() * 1000)))
        lease_ms = int(payload.get("lease_ms", 0))
        token = str(payload.get("token", ""))

        with self._cp_state_lock:
            self._expire_cp_lock_unlocked(lock_name, now_ms)
            state = self._cp_locks.get(lock_name)

            if action == "acquire":
                if not owner_id:
                    self._inc_stat("cp_lock_acquire_conflicts")
                    return {"acquired": False, "reason": "missing owner_id"}
                if lease_ms <= 0:
                    lease_ms = self._normalize_cp_lease_ms(None)
                if state is None:
                    fencing = int(self._cp_lock_fencing_counters.get(lock_name, 0)) + 1
                    self._cp_lock_fencing_counters[lock_name] = fencing
                    granted_token = token or str(payload.get("op_id", "")) or uuid.uuid4().hex
                    self._cp_locks[lock_name] = {
                        "owner_id": owner_id,
                        "token": granted_token,
                        "fencing_token": fencing,
                        "lease_deadline_ms": now_ms + lease_ms,
                        "reentrancy": 1,
                        "updated_at_ms": now_ms,
                    }
                    self._inc_stat("cp_lock_acquire_success")
                    return {
                        "acquired": True,
                        "owner_id": owner_id,
                        "token": granted_token,
                        "fencing_token": fencing,
                        "lease_deadline_ms": now_ms + lease_ms,
                        "reentrancy": 1,
                    }

                if (
                    self.config.cp.allow_reentrant_locks
                    and str(state.get("owner_id", "")) == owner_id
                    and str(state.get("token", "")) == token
                ):
                    reentrancy = int(state.get("reentrancy", 1)) + 1
                    state["reentrancy"] = reentrancy
                    state["lease_deadline_ms"] = now_ms + lease_ms
                    state["updated_at_ms"] = now_ms
                    self._inc_stat("cp_lock_acquire_success")
                    return {
                        "acquired": True,
                        "owner_id": owner_id,
                        "token": str(state["token"]),
                        "fencing_token": int(state["fencing_token"]),
                        "lease_deadline_ms": int(state["lease_deadline_ms"]),
                        "reentrancy": reentrancy,
                    }

                self._inc_stat("cp_lock_acquire_conflicts")
                return {
                    "acquired": False,
                    "owner_id": str(state.get("owner_id", "")),
                    "fencing_token": int(state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                }

            if action == "release":
                if state is None:
                    self._inc_stat("cp_lock_release_failures")
                    return {"released": False}
                if str(state.get("owner_id", "")) != owner_id or str(state.get("token", "")) != token:
                    self._inc_stat("cp_lock_release_failures")
                    return {"released": False}

                reentrancy = int(state.get("reentrancy", 1))
                if self.config.cp.allow_reentrant_locks and reentrancy > 1:
                    state["reentrancy"] = reentrancy - 1
                    state["updated_at_ms"] = now_ms
                    self._inc_stat("cp_lock_release_success")
                    return {"released": True, "remaining_reentrancy": reentrancy - 1}

                self._cp_locks.pop(lock_name, None)
                self._inc_stat("cp_lock_release_success")
                return {"released": True, "remaining_reentrancy": 0}

            if action == "refresh":
                if state is None:
                    self._inc_stat("cp_lock_refresh_failures")
                    return {"refreshed": False}
                if str(state.get("owner_id", "")) != owner_id or str(state.get("token", "")) != token:
                    self._inc_stat("cp_lock_refresh_failures")
                    return {"refreshed": False}
                if lease_ms <= 0:
                    lease_ms = self._normalize_cp_lease_ms(None)
                state["lease_deadline_ms"] = now_ms + lease_ms
                state["updated_at_ms"] = now_ms
                self._inc_stat("cp_lock_refresh_success")
                return {"refreshed": True, "lease_deadline_ms": int(state["lease_deadline_ms"])}

        raise ValueError(f"Unsupported cp_lock action: {action!r}")

    def _cp_lock_view(self, lock_name: str) -> dict[str, Any]:
        """Read local CP lock state view with lease-expiration cleanup."""
        now_ms = int(time.time() * 1000)
        with self._cp_state_lock:
            self._expire_cp_lock_unlocked(lock_name, now_ms)
            state = self._cp_locks.get(lock_name)
            if state is None:
                return {"locked": False}
            return {
                "locked": True,
                "owner_id": str(state.get("owner_id", "")),
                "token": str(state.get("token", "")),
                "fencing_token": int(state.get("fencing_token", 0)),
                "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                "reentrancy": int(state.get("reentrancy", 1)),
            }

    def _cp_lock_acquire(
        self,
        lock_name: str,
        *,
        owner_id: str,
        blocking: bool,
        timeout_seconds: float | None,
        lease_seconds: float | None,
        current_token: str | None = None,
    ) -> dict[str, Any]:
        """Acquire CP lock, optionally retrying until timeout."""
        if not self.config.cp.enabled:
            raise ClusterNotRunningError("CP subsystem is disabled.")
        lease_ms = self._normalize_cp_lease_ms(lease_seconds)
        deadline: float | None = None
        if blocking and timeout_seconds is not None:
            deadline = time.monotonic() + max(0.0, float(timeout_seconds))

        token = current_token or uuid.uuid4().hex
        while True:
            result = self._submit_local_operation(
                collection="cp_lock",
                name=lock_name,
                action="acquire",
                values={
                    "owner_id": owner_id,
                    "token": token,
                    "lease_ms": lease_ms,
                    "now_ms": int(time.time() * 1000),
                },
            )
            if isinstance(result, dict) and bool(result.get("acquired")):
                return result
            if not blocking:
                return {"acquired": False}
            if deadline is not None and time.monotonic() >= deadline:
                return {"acquired": False}
            time.sleep(self.config.cp.lock_retry_interval_seconds)

    def _cp_lock_release(self, lock_name: str, *, owner_id: str, token: str) -> bool:
        """Release CP lock ownership."""
        if not self.config.cp.enabled:
            return False
        result = self._submit_local_operation(
            collection="cp_lock",
            name=lock_name,
            action="release",
            values={"owner_id": owner_id, "token": token, "now_ms": int(time.time() * 1000)},
        )
        return bool(isinstance(result, dict) and result.get("released"))

    def _cp_lock_refresh(
        self,
        lock_name: str,
        *,
        owner_id: str,
        token: str,
        lease_seconds: float | None,
    ) -> bool:
        """Extend CP lock lease for active owner session."""
        if not self.config.cp.enabled:
            return False
        lease_ms = self._normalize_cp_lease_ms(lease_seconds)
        result = self._submit_local_operation(
            collection="cp_lock",
            name=lock_name,
            action="refresh",
            values={
                "owner_id": owner_id,
                "token": token,
                "lease_ms": lease_ms,
                "now_ms": int(time.time() * 1000),
            },
        )
        return bool(isinstance(result, dict) and result.get("refreshed"))

    def _cp_lock_is_locked(self, lock_name: str) -> bool:
        """Return local view of lock ownership state."""
        return bool(self._cp_lock_view(lock_name).get("locked"))

    def _cp_lock_owner(self, lock_name: str) -> str | None:
        """Return local view of lock owner ID."""
        view = self._cp_lock_view(lock_name)
        owner_id = view.get("owner_id")
        return str(owner_id) if isinstance(owner_id, str) and owner_id else None

    def _cp_snapshot_payload(self) -> dict[str, Any]:
        """Build serializable snapshot for CP lock subsystem state."""
        with self._cp_state_lock:
            locks = {
                name: {
                    "owner_id": str(state.get("owner_id", "")),
                    "token": str(state.get("token", "")),
                    "fencing_token": int(state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(state.get("lease_deadline_ms", 0)),
                    "reentrancy": int(state.get("reentrancy", 1)),
                    "updated_at_ms": int(state.get("updated_at_ms", 0)),
                }
                for name, state in self._cp_locks.items()
            }
            counters = {
                name: int(counter) for name, counter in self._cp_lock_fencing_counters.items()
            }
        return {"locks": locks, "fencing_counters": counters}

    def _load_cp_snapshot_payload(self, payload: dict[str, Any]) -> None:
        """Load CP lock snapshot payload into runtime memory."""
        raw_locks = payload.get("locks", {})
        raw_counters = payload.get("fencing_counters", {})
        parsed_locks: dict[str, dict[str, Any]] = {}
        if isinstance(raw_locks, dict):
            for name, raw_state in raw_locks.items():
                if not isinstance(raw_state, dict):
                    continue
                parsed_locks[str(name)] = {
                    "owner_id": str(raw_state.get("owner_id", "")),
                    "token": str(raw_state.get("token", "")),
                    "fencing_token": int(raw_state.get("fencing_token", 0)),
                    "lease_deadline_ms": int(raw_state.get("lease_deadline_ms", 0)),
                    "reentrancy": max(1, int(raw_state.get("reentrancy", 1))),
                    "updated_at_ms": int(raw_state.get("updated_at_ms", 0)),
                }
        parsed_counters: dict[str, int] = {}
        if isinstance(raw_counters, dict):
            for name, counter in raw_counters.items():
                try:
                    parsed_counters[str(name)] = int(counter)
                except (TypeError, ValueError):
                    continue
        with self._cp_state_lock:
            self._cp_locks = parsed_locks
            self._cp_lock_fencing_counters = parsed_counters
