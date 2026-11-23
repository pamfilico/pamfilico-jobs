"""A worker: one process that connects OUT to jobs, claims runs of its app's tasks and runs them.

    jobs.worker("core", slots=10).discover("app.domains").start()

- Registers its tasks (and their crons/events) on start, and again every minute.
- Long-polls for work; each run executes in a thread (async tasks get their own event loop).
- Heartbeats every run while it executes. If jobs answers `lost` (lease expired, timed out), the
  task's ctx.cancelled() turns True and its result is thrown away: someone else owns the run.
- SIGTERM/SIGINT: stop claiming, let running tasks finish (JOBS_SHUTDOWN_GRACE_SECONDS, default
  30), deregister, exit. Anything still running is reclaimed by jobs when its lease runs out.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import pkgutil
import signal
import socket
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor

from pamfilico_jobs import context as _ctx
from pamfilico_jobs._version import __version__
from pamfilico_jobs.context import Context
from pamfilico_jobs.errors import Cancelled, ChildRunFailed, JobsError, JobsUnavailable, NonRetryable

logger = logging.getLogger("pamfilico_jobs.worker")

REREGISTER_S = 60


class Worker:
    def __init__(self, jobs, app: str, slots: int = 10, labels: dict | None = None, name: str | None = None):
        self.jobs = jobs
        self.app = app
        self.slots = max(1, int(slots))
        self.labels = {str(k): str(v) for k, v in (labels or {}).items()}
        self.name = name or os.getenv("JOBS_WORKER_NAME") or f"{app}@{socket.gethostname()}"
        self.worker_id = f"{app}-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"[:100]
        self.only_tasks: list[str] | None = None
        self._stop = threading.Event()
        self._slot_freed = threading.Event()
        self._active: dict[str, Context] = {}
        self._lock = threading.Lock()
        self.grace_s = float(os.getenv("JOBS_SHUTDOWN_GRACE_SECONDS", "30"))

    # ── setup ──────────────────────────────────────────────────────────────────────────────

    def discover(self, *packages: str) -> "Worker":
        """Import every module named `jobs` (and every module inside a `jobs` package) under the
        given packages, so their @jobs.task declarations register. e.g. discover("app.domains")."""
        for package in packages:
            pkg = importlib.import_module(package)
            if not hasattr(pkg, "__path__"):
                continue
            for info in pkgutil.walk_packages(pkg.__path__, prefix=pkg.__name__ + "."):
                parts = info.name.split(".")
                if parts[-1] == "jobs" or "jobs" in parts[len(package.split(".")):-1]:
                    importlib.import_module(info.name)
        return self

    def include(self, *modules: str) -> "Worker":
        for m in modules:
            importlib.import_module(m)
        return self

    def tasks(self, *names: str) -> "Worker":
        """Only claim these tasks (default: every task of the app this process declares)."""
        self.only_tasks = list(names)
        return self

    def _task_defs(self):
        defs = self.jobs.registry.for_app(self.app)
        if self.only_tasks is not None:
            defs = [d for d in defs if d.name in self.only_tasks]
        return defs

    def register(self) -> None:
        defs = self._task_defs()
        if not defs:
            raise RuntimeError(
                f"no tasks named {self.app}.* are declared in this process: import the modules that "
                f"declare them (worker.discover('app.domains') or worker.include('app.x.jobs'))"
            )
        self.jobs.http.request(
            "POST",
            "/workers/register",
            json_body={
                "worker_id": self.worker_id,
                "name": self.name,
                "hostname": socket.gethostname(),
                "labels": self.labels,
                "slots": self.slots,
                "sdk_version": __version__,
                "tasks": [d.definition() for d in defs],
            },
            attempts=5,
        )
        self._registered_at = time.monotonic()
        logger.info("worker %s registered %d task(s): %s", self.worker_id, len(defs), ", ".join(d.name for d in defs))

    # ── the loop ───────────────────────────────────────────────────────────────────────────

    def start(self, install_signal_handlers: bool = True) -> None:
        """Run until SIGTERM/SIGINT (or stop()). Blocks."""
        if install_signal_handlers and threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, lambda *_: self.stop())
            signal.signal(signal.SIGINT, lambda *_: self.stop())
        self._register_until_ok()
        pool = ThreadPoolExecutor(max_workers=self.slots, thread_name_prefix=f"jobs-{self.app}")
        backoff = 1.0
        try:
            while not self._stop.is_set():
                if time.monotonic() - self._registered_at > REREGISTER_S:
                    self._safe_register()
                with self._lock:
                    free = self.slots - len(self._active)
                if free <= 0:
                    self._slot_freed.wait(1.0)
                    self._slot_freed.clear()
                    continue
                try:
                    payload = self.jobs.http.request(
                        "POST",
                        f"/workers/{self.worker_id}/claim",
                        json_body={"max": free, "wait_s": 10 if free == self.slots else 1},
                        timeout=40,
                    )
                    backoff = 1.0
                except JobsError as exc:
                    if exc.code == "worker_unknown":
                        self._safe_register()
                        continue
                    if isinstance(exc, JobsUnavailable) or exc.status >= 500 or exc.status == 0:
                        logger.warning("jobs unreachable, retrying in %.0fs: %s", backoff, exc.message)
                        self._stop.wait(backoff)
                        backoff = min(backoff * 2, 30)
                        continue
                    logger.error("claim refused: %s", exc)
                    self._stop.wait(5)
                    continue
                runs = payload.get("data") or []
                for run in runs:
                    ctx = Context(run)
                    with self._lock:
                        self._active[run["id"]] = ctx
                    pool.submit(self._execute, run, ctx)
                if not runs and free != self.slots:
                    self._slot_freed.wait(1.0)
                    self._slot_freed.clear()
        finally:
            self._shutdown(pool)

    def stop(self) -> None:
        if not self._stop.is_set():
            logger.info("worker %s stopping: no new runs; waiting for %d running", self.worker_id, len(self._active))
        self._stop.set()

    def _shutdown(self, pool: ThreadPoolExecutor) -> None:
        deadline = time.monotonic() + self.grace_s
        while time.monotonic() < deadline:
            with self._lock:
                if not self._active:
                    break
            time.sleep(0.2)
        pool.shutdown(wait=False, cancel_futures=True)
        try:
            self.jobs.http.request("POST", f"/workers/{self.worker_id}/stop")
        except Exception:  # noqa: BLE001
            pass
        logger.info("worker %s stopped", self.worker_id)

    def _register_until_ok(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            try:
                self.register()
                return
            except JobsError as exc:
                if not isinstance(exc, JobsUnavailable) and exc.status not in (0, 502, 503, 504):
                    raise
                logger.warning("could not register with jobs (%s); retrying in %.0fs", exc.message, delay)
                self._stop.wait(delay)
                delay = min(delay * 2, 30)

    def _safe_register(self) -> None:
        try:
            self.register()
        except Exception as exc:  # noqa: BLE001
            logger.warning("re-register failed: %s", exc)
            self._registered_at = time.monotonic()

    # ── one run ────────────────────────────────────────────────────────────────────────────

    def _execute(self, run: dict, ctx: Context) -> None:
        run_id, token = run["id"], run.get("claim_token")
        beat_now = threading.Event()
        done = threading.Event()
        ctx._on_progress = beat_now.set
        lease_s = _lease_seconds(run)
        interval = max(0.5, min(15.0, lease_s / 4))
        beater = threading.Thread(
            target=self._heartbeat, args=(run_id, token, ctx, beat_now, done, interval), daemon=True, name=f"hb-{run_id[:8]}"
        )
        beater.start()
        reset = _ctx._current.set(ctx)
        started = time.monotonic()
        try:
            tdef = self.jobs.registry.get(run["task"])
            if tdef is None:
                raise NonRetryable(f"this worker has no task {run['task']!r}")
            inp = run.get("input") or {}
            if tdef.is_async:
                output = asyncio.run(tdef.call(inp, ctx))
            else:
                output = tdef.call(inp, ctx)
            if asyncio.iscoroutine(output):
                output = asyncio.run(output)
            done.set()
            if ctx.lost:
                logger.warning("%s %s finished after jobs took it back; result dropped", run["task"], run_id)
                return
            self._report(run_id, "complete", {"claim_token": token, "output": _jsonable(output)}, ctx)
            logger.info("%s %s done in %.2fs", run["task"], run_id, time.monotonic() - started)
        except BaseException as exc:  # noqa: BLE001 - everything becomes a report
            done.set()
            if ctx.lost:
                return
            if isinstance(exc, (KeyboardInterrupt, SystemExit)) and not isinstance(exc, Cancelled):
                body = {"claim_token": token, "error": f"worker interrupted: {type(exc).__name__}", "error_type": "interrupted", "retryable": True}
                self._report(run_id, "fail", body, ctx)
                raise
            cancelled = isinstance(exc, Cancelled) or (ctx.cancelled() and not ctx.lost)
            body = {
                "claim_token": token,
                "error": _format_error(exc),
                "error_type": "cancelled" if cancelled else _error_type(exc),
                "retryable": not isinstance(exc, NonRetryable),
            }
            self._report(run_id, "fail", body, ctx)
            log = logger.info if cancelled else logger.warning
            log("%s %s attempt %s/%s %s: %s", run["task"], run_id, ctx.attempt, ctx.max_attempts,
                "cancelled" if cancelled else "failed", exc)
        finally:
            done.set()
            beat_now.set()
            _ctx._current.reset(reset)
            with self._lock:
                self._active.pop(run_id, None)
            self._slot_freed.set()

    def _heartbeat(self, run_id, token, ctx: Context, beat_now: threading.Event, done: threading.Event, interval: float):
        while not done.is_set():
            beat_now.wait(interval)
            beat_now.clear()
            if done.is_set():
                return
            try:
                data = self.jobs.http.request(
                    "POST",
                    f"/runs/{run_id}/heartbeat",
                    json_body={"claim_token": token, "progress": ctx.progress_value, "message": ctx.progress_message},
                )["data"]
                if data.get("cancel_requested"):
                    ctx._cancel.set()
            except JobsError as exc:
                if exc.code == "lost":
                    ctx._lost.set()
                    ctx._cancel.set()
                    logger.warning("lost run %s (%s)", run_id, exc.message)
                    return
                logger.warning("heartbeat for %s failed: %s", run_id, exc.message)

    def _report(self, run_id: str, what: str, body: dict, ctx: Context) -> None:
        try:
            self.jobs.http.request("POST", f"/runs/{run_id}/{what}", json_body=body, attempts=6)
        except JobsError as exc:
            if exc.code == "lost":
                logger.warning("run %s was taken back before it could report %s", run_id, what)
            elif exc.code == "too_large" and what == "complete":
                self._report(
                    run_id,
                    "fail",
                    {"claim_token": body["claim_token"], "error": exc.message, "error_type": "OutputTooLarge", "retryable": False},
                    ctx,
                )
            else:
                logger.error("could not report %s for %s: %s", what, run_id, exc)


def _lease_seconds(run: dict) -> float:
    try:
        if run.get("lease_s"):
            return float(run["lease_s"])
    except (TypeError, ValueError):
        pass
    return 60.0


def _error_type(exc: BaseException) -> str:
    if isinstance(exc, ChildRunFailed):
        return "child_failed"
    return type(exc).__name__


def _format_error(exc: BaseException) -> str:
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return text[-15000:]


def _jsonable(value):
    if value is None:
        return None
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return json.loads(json.dumps(value, default=str))
