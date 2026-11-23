from __future__ import annotations

from typing import Any

TERMINAL = ("done", "failed", "cancelled")


class Run:
    """A run as jobs reports it. Attribute access over the JSON (`run.status`, `run.output`, …);
    the whole dict is `run.raw`."""

    __slots__ = ("raw",)

    def __init__(self, raw: dict):
        object.__setattr__(self, "raw", dict(raw or {}))

    def __getattr__(self, name: str) -> Any:
        try:
            return self.raw[name]
        except KeyError:
            if name in _KNOWN:
                return None
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        raise AttributeError("Run is read-only; fetch it again with jobs.get(run.id)")

    @property
    def finished(self) -> bool:
        return self.raw.get("status") in TERMINAL

    @property
    def ok(self) -> bool:
        return self.raw.get("status") == "done"

    @property
    def deduplicated(self) -> bool:
        return bool(self.raw.get("deduplicated"))

    def to_dict(self) -> dict:
        return dict(self.raw)

    def __repr__(self) -> str:
        return f"<Run {self.raw.get('task')} {self.raw.get('id')} {self.raw.get('status')}>"

    def __eq__(self, other) -> bool:
        return isinstance(other, Run) and other.raw.get("id") == self.raw.get("id")

    def __hash__(self) -> int:
        return hash(self.raw.get("id"))


_KNOWN = {
    "id", "task", "app", "triggered_by", "trigger_kind", "status", "input", "output", "error",
    "error_type", "attempt", "max_attempts", "timeout_s", "priority", "labels", "client_id",
    "subject", "user_id", "key", "parent_run_id", "root_run_id", "retry_of", "event_id",
    "schedule_id", "progress", "progress_message", "cancel_requested", "worker_id", "sequence",
    "available_at", "created_at", "started_at", "finished_at", "updated_at", "duration_s",
    "claim_token", "lease_expires_at",
}
