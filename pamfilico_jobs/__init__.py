"""pamfilico-jobs: the Python SDK for jobs (the central job plane).

    from pamfilico_jobs import jobs, NonRetryable

    @jobs.task("core.reports.build", fair_by="client_id", one_open_per="subject", timeout="5m")
    def build(inp, ctx):
        ctx.progress(10, "loading")
        ...
        return {"report_id": 42}          # a small JSON reference, not the data

    jobs.trigger("core.reports.build", {"month": "2026-11"}, client_id=cid, subject=f"report:{cid}")
    jobs.worker("core", slots=10).discover("app.domains").start()     # in worker_jobs.py

Docs: https://github.com/pamfilico/pamfilico-jobs
"""

from pamfilico_jobs._version import __version__
from pamfilico_jobs.client import Jobs
from pamfilico_jobs.context import Context, current
from pamfilico_jobs.errors import (
    LimitReached,
    Cancelled,
    ChildRunFailed,
    InvalidSignature,
    JobsError,
    JobsUnavailable,
    NonRetryable,
    RunFailed,
    WaitTimeout,
)
from pamfilico_jobs.run import Run

#: The default client, configured from JOBS_API_URL / JOBS_API_KEY / PAMFILICO_JOBS_MODE.
jobs = Jobs()

__all__ = [
    "jobs",
    "Jobs",
    "Run",
    "Context",
    "current",
    "NonRetryable",
    "Cancelled",
    "ChildRunFailed",
    "RunFailed",
    "WaitTimeout",
    "JobsError",
    "JobsUnavailable",
    "LimitReached",
    "InvalidSignature",
    "__version__",
]
