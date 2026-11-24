from __future__ import annotations


class JobsError(Exception):
    """The jobs API said no. `status` is the HTTP status, `code` a stable machine code."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"[{status} {code}] {message}")
        self.status = status
        self.code = code
        self.message = message


class LimitReached(JobsError):
    """A trigger refused because the client already has ``max_open`` open runs of that task.

    Nothing was queued. It is not a failure of jobs: it is the limit you asked for, working.
    Catch it and try again later (the next tick), or skip.
    """

    def __init__(self, message: str):
        super().__init__(409, "limit_reached", message)


class JobsUnavailable(JobsError):
    """jobs could not be reached (network error, timeout, 5xx)."""

    def __init__(self, message: str):
        super().__init__(503, "unavailable", message)


class NonRetryable(Exception):
    """Raise inside a task to fail the run NOW, without using its remaining retries.

    Use it for errors another attempt cannot fix: bad input, a 400 from an API, a missing row.
    """


class Cancelled(Exception):
    """Raised by `ctx.raise_if_cancelled()` when someone cancelled the run."""


class RunFailed(Exception):
    """A run you waited for (run_child / wait(raise_on_failure=True)) did not finish `done`."""

    def __init__(self, run):
        self.run = run
        super().__init__(f"run {run.id} ({run.task}) ended {run.status}: {run.error_type or ''} {run.error or ''}".strip())


class ChildRunFailed(RunFailed):
    """A child started with `jobs.run_child` failed or was cancelled. Retryable by default:
    the parent's next attempt gets the SAME child back and re-runs it."""


class WaitTimeout(Exception):
    def __init__(self, run):
        self.run = run
        super().__init__(f"run {run.id} ({run.task}) still {run.status} after the wait timeout")


class InvalidSignature(Exception):
    """A webhook's signature or timestamp did not check out."""
