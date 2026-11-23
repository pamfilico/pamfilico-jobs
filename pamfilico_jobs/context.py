from __future__ import annotations

import contextvars
import threading
import time

from pamfilico_jobs.errors import Cancelled

_current: contextvars.ContextVar = contextvars.ContextVar("pamfilico_jobs_ctx", default=None)


def current():
    """The Context of the task running in this thread/coroutine, or None outside a task."""
    return _current.get()


class Context:
    """What a task knows about its run. Passed as the second argument: `def task(inp, ctx)`."""

    def __init__(self, run: dict, *, on_progress=None):
        self.run_id: str = run.get("id")
        self.task: str = run.get("task")
        self.attempt: int = int(run.get("attempt") or 1)
        self.max_attempts: int = int(run.get("max_attempts") or 1)
        self.client_id = run.get("client_id")
        self.subject = run.get("subject")
        self.user_id = run.get("user_id")
        self.key = run.get("key")
        self.parent_run_id = run.get("parent_run_id")
        self.root_run_id = run.get("root_run_id")
        self.trigger_kind = run.get("trigger_kind")
        self.input = run.get("input") or {}
        self._on_progress = on_progress
        self._cancel = threading.Event()
        self._lost = threading.Event()
        self.progress_value = None
        self.progress_message = None
        self._last_sent = 0.0

    @property
    def is_last_attempt(self) -> bool:
        return self.attempt >= self.max_attempts

    def progress(self, percent=None, message=None) -> None:
        """Report progress. Sent with the next heartbeat (at most about once a second)."""
        if percent is not None:
            self.progress_value = max(0, min(100, int(percent)))
        if message is not None:
            self.progress_message = str(message)
        if self._on_progress and time.monotonic() - self._last_sent > 1.0:
            self._last_sent = time.monotonic()
            self._on_progress()

    def cancelled(self) -> bool:
        """True once someone cancelled the run (or the worker lost it). Check it in long loops."""
        return self._cancel.is_set()

    def raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            raise Cancelled(f"run {self.run_id} was cancelled")

    @property
    def lost(self) -> bool:
        """True if jobs took the run back (lease expired / timed out). Results will be ignored."""
        return self._lost.is_set()

    def __repr__(self) -> str:
        return f"<Context {self.task} {self.run_id} attempt {self.attempt}/{self.max_attempts}>"
