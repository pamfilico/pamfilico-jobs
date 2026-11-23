"""`jobs`: the one object an app uses. Declare tasks, start runs, read them, run a worker.

    from pamfilico_jobs import jobs

    @jobs.task("core.signals.run", fair_by="client_id", one_open_per="subject", timeout="5m")
    def run(inp, ctx): ...

    run = jobs.trigger("core.signals.run", {"lead_id": 7}, client_id="c1", subject="lead:7")
    jobs.wait(run.id, timeout=30)

Configuration comes from the environment (or `jobs.configure(...)`):
    JOBS_API_URL         https://api.jobs.pamfili.co/api/v1
    JOBS_API_KEY         jb_…  (from the jobs console: Apps & keys)
    PAMFILICO_JOBS_MODE  "inline" → trigger() runs the task in-process (tests); default "remote"
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Callable, Iterable

from pamfilico_jobs import context as _ctx
from pamfilico_jobs._version import __version__
from pamfilico_jobs.durations import seconds
from pamfilico_jobs.errors import ChildRunFailed, JobsError, JobsUnavailable, RunFailed, WaitTimeout
from pamfilico_jobs.run import Run
from pamfilico_jobs.tasks import Registry, make_task
from pamfilico_jobs.transport import Transport

SERVER_WAIT_S = 20


class Jobs:
    def __init__(self, url: str | None = None, key: str | None = None, mode: str | None = None):
        self.registry = Registry()
        self._url, self._key, self._mode = url, key, mode
        self._transport: Transport | None = None
        self._inline = None

    # ── configuration ──────────────────────────────────────────────────────────────────────

    def configure(self, url: str | None = None, key: str | None = None, mode: str | None = None) -> "Jobs":
        if url is not None:
            self._url = url
        if key is not None:
            self._key = key
        if mode is not None:
            self._mode = mode
        self._transport = None
        return self

    @property
    def mode(self) -> str:
        return (self._mode or os.getenv("PAMFILICO_JOBS_MODE") or "remote").strip().lower()

    @property
    def inline(self) -> bool:
        return self.mode == "inline"

    @property
    def http(self) -> Transport:
        url = self._url or os.getenv("JOBS_API_URL", "")
        key = self._key or os.getenv("JOBS_API_KEY", "")
        t = self._transport
        if t is None or t.base_url != url.rstrip("/") or t.api_key != key:
            t = self._transport = Transport(url, key, f"pamfilico-jobs-python/{__version__}")
        return t

    def _inline_runner(self):
        if self._inline is None:
            from pamfilico_jobs.inline import InlineRunner

            self._inline = InlineRunner(self)
        return self._inline

    # ── declaring tasks ────────────────────────────────────────────────────────────────────

    def task(
        self,
        name: str,
        *,
        cron: str | None = None,
        on_events: Iterable[str] | str | None = None,
        fair_by: str | None = None,
        one_open_per: str | None = None,
        timeout="10m",
        retries: int | None = None,
        backoff=None,
        labels: dict | None = None,
        concurrency: int | None = None,
        priority: int = 0,
        description: str | None = None,
        idempotent: bool = True,
        require_key: bool | None = None,
        retention=None,
        queue_ttl=None,
    ) -> Callable:
        """Declare a task. The function stays a plain function (call it directly in unit tests).

        name          <app>.<domain>.<action>; the app's worker runs every <app>.* task
        cron          "*/5 * * * *" (a 6th field is seconds); runs never overlap
        on_events     event names (globs allowed) that start a run with the event payload as input
        fair_by       "client_id": one client's backlog can't starve the others
        one_open_per  "subject": never two open runs of this task for the same subject
        timeout       per attempt ("30s", "10m", "1h")
        retries       attempts after the first; backoff e.g. ["10s", "1m", "5m"] (last repeats)
        labels        worker labels this task needs, e.g. {"gpu": "true"}
        concurrency   at most N running at once, across every worker
        priority      higher first
        idempotent    False = running it twice has a cost (a charge, an email, a paid call):
                      triggers must pass a key, retries default to 0, and if a worker dies or
                      times out mid-run the run is NOT retried: it fails as `outcome_unknown`
                      and shows up in the console under "Needs review"
        require_key   refuse triggers without a key (default: True when not idempotent)
        retention     how long the finished run's record is kept ("7d"; default: the server's)
        queue_ttl     how long a run may wait for a worker before it expires unrun ("10m")
        """

        def decorator(fn):
            tdef = make_task(
                name,
                fn,
                cron=cron,
                on_events=on_events,
                fair_by=fair_by,
                one_open_per=one_open_per,
                timeout=timeout,
                retries=retries,
                backoff=backoff,
                labels=labels,
                concurrency=concurrency,
                priority=priority,
                description=description,
                idempotent=idempotent,
                require_key=require_key,
                retention=retention,
                queue_ttl=queue_ttl,
            )
            self.registry.add(tdef)
            fn.task = tdef
            return fn

        return decorator

    # ── starting work ──────────────────────────────────────────────────────────────────────

    def trigger(
        self,
        task: str,
        input: dict | None = None,
        *,
        client_id=None,
        subject=None,
        user_id=None,
        key: str | None = None,
        priority: int | None = None,
        delay=None,
        run_at=None,
        parent_run_id: str | None = None,
    ) -> Run:
        """Queue a run of `task` (any app's). Same `key` → the same run back (`run.deduplicated`).

        delay   "30s" / "10m" / seconds: not claimable before then
        run_at  a datetime (or ISO string): not claimable before that moment
        """
        if self.inline:
            return self._inline_runner().trigger(
                task, input, client_id=client_id, subject=subject, user_id=user_id, key=key, parent_run_id=parent_run_id
            )
        body = {
            "task": task,
            "input": input or {},
            "client_id": _s(client_id),
            "subject": _s(subject),
            "user_id": _s(user_id),
            "key": key,
            "priority": priority,
            "delay_s": seconds(delay),
            "run_at": run_at.isoformat() if hasattr(run_at, "isoformat") else run_at,
            "parent_run_id": parent_run_id,
        }
        ctx = _ctx.current()
        if ctx is not None and not parent_run_id:
            # Triggered from inside a task: the new run joins this run's trace.
            body["source_run_id"] = ctx.run_id
        # Retrying a trigger is only safe when it is idempotent.
        payload = self.http.request("POST", "/runs", json_body=body, attempts=3 if key else 1)
        return Run(payload["data"])

    def emit(self, name: str, payload: dict | None = None, *, client_id=None, subject=None, user_id=None) -> list[Run]:
        """Emit an event: every task whose `on_events` matches starts a run. Returns those runs."""
        if self.inline:
            return self._inline_runner().emit(name, payload, client_id=client_id, subject=subject, user_id=user_id)
        ctx = _ctx.current()
        data = self.http.request(
            "POST",
            "/events",
            json_body={
                "name": name, "payload": payload or {}, "client_id": _s(client_id), "subject": _s(subject),
                "user_id": _s(user_id), "source_run_id": ctx.run_id if ctx is not None else None,
            },
        )["data"]
        return [Run(r) for r in data.get("runs", [])]

    # ── reading ────────────────────────────────────────────────────────────────────────────

    def get(self, run_id: str) -> Run:
        if self.inline:
            return self._inline_runner().get(run_id)
        return Run(self.http.request("GET", f"/runs/{run_id}", attempts=3)["data"])

    def runs(self, **filters) -> list[Run]:
        """Filters: task (glob ok), status ("failed" or "queued,running"), client_id, subject,
        user_id, key, parent_run_id, root_run_id, retry_of, trigger_kind, since, limit, offset."""
        if self.inline:
            return self._inline_runner().runs(**filters)
        params = {k: v for k, v in filters.items() if v is not None}
        return [Run(r) for r in self.http.request("GET", "/runs", params=params, attempts=3)["data"]]

    def logs(self, run_id: str) -> list[dict]:
        if self.inline:
            return []
        return self.http.request("GET", f"/runs/{run_id}/logs", attempts=3)["data"]

    def wait(self, run_id: str, timeout=60, raise_on_failure: bool = False) -> Run:
        """Block until the run finishes (long-polls jobs). WaitTimeout if it doesn't in time."""
        if self.inline:
            run = self._inline_runner().get(run_id)
        else:
            run = self._wait_remote(run_id, seconds(timeout) or 60)
        if not run.finished:
            raise WaitTimeout(run)
        if raise_on_failure and not run.ok:
            raise RunFailed(run)
        return run

    def _wait_remote(self, run_id: str, timeout: float, ctx=None) -> Run:
        deadline = time.monotonic() + timeout
        backoff = 1.0
        while True:
            left = deadline - time.monotonic()
            try:
                run = Run(
                    self.http.request(
                        "GET",
                        f"/runs/{run_id}/wait",
                        params={"timeout": max(0, min(SERVER_WAIT_S, left))},
                        timeout=SERVER_WAIT_S + 10,
                    )["data"]
                )
                backoff = 1.0
            except JobsUnavailable:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue
            if run.finished or time.monotonic() >= deadline:
                return run
            if ctx is not None and ctx.cancelled():
                return run

    def cancel(self, run_id: str) -> Run:
        if self.inline:
            return self._inline_runner().get(run_id)
        return Run(self.http.request("POST", f"/runs/{run_id}/cancel", attempts=3)["data"])

    def retry(self, run_id: str, input: dict | None = None) -> Run:
        """A NEW run of the same task and input (or `input`), with `retry_of` = the old run."""
        if self.inline:
            old = self._inline_runner().get(run_id)
            return self._inline_runner().trigger(old.task, input or old.input, client_id=old.client_id, subject=old.subject, user_id=old.user_id)
        body = {"input": input} if input is not None else {}
        return Run(self.http.request("POST", f"/runs/{run_id}/retry", json_body=body)["data"])

    def trace(self, trace_or_run_id: str) -> dict:
        """Everything one action caused, across apps: runs (with their history), the events
        between them and the webhook deliveries. Pass a trace id or any run id in it."""
        if self.inline:
            return {"trace_id": trace_or_run_id, "runs": [r.raw for r in self._inline_runner().runs(limit=1000)]}
        return self.http.request("GET", f"/traces/{trace_or_run_id}", attempts=3)["data"]

    def whoami(self) -> dict:
        return self.http.request("GET", "/whoami", attempts=3)["data"]

    # ── children (inside a task) ───────────────────────────────────────────────────────────

    def _child_key(self, ctx, task: str, input: dict | None, key: str | None) -> str:
        if key:
            return key
        digest = hashlib.sha256(json.dumps(input or {}, sort_keys=True, default=str).encode()).hexdigest()[:16]
        return f"child:{ctx.run_id}:{task}:{digest}"

    def run_child_sync(self, task: str, input: dict | None = None, *, timeout=None, key=None, **trigger_kw):
        """Start a child run and wait for it; return its output. Raise ChildRunFailed if it fails.

        The child gets a key derived from (this run, task, input), so when THIS run is retried the
        same child comes back: a finished child is not run again, a failed one runs again.
        """
        ctx = _ctx.current()
        if ctx is None:
            raise RuntimeError("run_child only works inside a task; outside one use trigger() + wait()")
        if self.inline:
            return self._inline_runner().run_child_sync(task, input, ctx, **trigger_kw)
        run = self.trigger(task, input, key=self._child_key(ctx, task, input, key), parent_run_id=ctx.run_id, **trigger_kw)
        limit = seconds(timeout) or 24 * 3600
        run = self._wait_remote(run.id, limit, ctx)
        if not run.finished:
            if ctx.cancelled():
                self.cancel(run.id)
                ctx.raise_if_cancelled()
            raise WaitTimeout(run)
        if not run.ok:
            raise ChildRunFailed(run)
        return run.output

    async def run_child(self, task: str, input: dict | None = None, **kw):
        """`await jobs.run_child(...)` inside an async task. See run_child_sync."""
        ctx = _ctx.current()
        if ctx is None:
            raise RuntimeError("run_child only works inside a task; outside one use trigger() + wait()")
        if self.inline:
            return await self._inline_runner().run_child_async(task, input, ctx, **kw)
        return await asyncio.to_thread(_with_ctx(ctx, self.run_child_sync), task, input, **kw)

    def run_children_sync(self, task: str, inputs: list, **kw) -> list:
        """Start one child per input, wait for all; outputs in order. If any failed, raises the
        first ChildRunFailed after every child has finished."""
        ctx = _ctx.current()
        if ctx is None:
            raise RuntimeError("run_children only works inside a task")
        if self.inline:
            return [self._inline_runner().run_child_sync(task, i, ctx) for i in inputs]
        runs = [
            self.trigger(task, i, key=self._child_key(ctx, task, i, None), parent_run_id=ctx.run_id)
            for i in inputs
        ]
        limit = seconds(kw.get("timeout")) or 24 * 3600
        finished = [self._wait_remote(r.id, limit, ctx) for r in runs]
        for r in finished:
            if not r.finished:
                raise WaitTimeout(r)
        for r in finished:
            if not r.ok:
                raise ChildRunFailed(r)
        return [r.output for r in finished]

    async def run_children(self, task: str, inputs: list, **kw) -> list:
        ctx = _ctx.current()
        if ctx is None:
            raise RuntimeError("run_children only works inside a task")
        if self.inline:
            return [await self._inline_runner().run_child_async(task, i, ctx) for i in inputs]
        return await asyncio.to_thread(_with_ctx(ctx, self.run_children_sync), task, inputs, **kw)

    # ── webhooks (the app's own subscriptions) ─────────────────────────────────────────────

    def create_webhook(self, url: str, events=("run.*",), task_filter: str | None = None, description=None) -> dict:
        """Returns the webhook with its `secret` (shown ONCE: store it as JOBS_WEBHOOK_SECRET)."""
        return self.http.request(
            "POST",
            "/webhooks",
            json_body={"url": url, "events": list(events), "task_filter": task_filter, "description": description},
        )["data"]

    def webhooks(self) -> list[dict]:
        return self.http.request("GET", "/webhooks", attempts=3)["data"]

    def delete_webhook(self, webhook_id: str) -> None:
        self.http.request("DELETE", f"/webhooks/{webhook_id}")

    # ── worker ─────────────────────────────────────────────────────────────────────────────

    def worker(self, app: str, slots: int = 10, labels: dict | None = None, name: str | None = None):
        from pamfilico_jobs.worker import Worker

        return Worker(self, app, slots=slots, labels=labels, name=name)


def _s(value):
    return None if value is None else str(value)


def _with_ctx(ctx, fn):
    """Run fn in a worker thread with the task's Context set there too."""

    def wrapper(*a, **kw):
        token = _ctx._current.set(ctx)
        try:
            return fn(*a, **kw)
        finally:
            _ctx._current.reset(token)

    return wrapper


__all__ = ["Jobs", "JobsError"]
