"""PAMFILICO_JOBS_MODE=inline: trigger() runs the task right here, synchronously. For tests.

What it keeps: task lookup, (input, ctx) calling, async tasks, children, events, idempotency keys,
statuses (done / failed with the error text). What it does not do: retries, timeouts, crons,
workers, webhooks — those belong to the engine and are covered by its integration tests.
"""

from __future__ import annotations

import asyncio
import fnmatch
import traceback
import uuid
from datetime import datetime, timezone

from pamfilico_jobs import context as _ctx
from pamfilico_jobs.context import Context
from pamfilico_jobs.errors import ChildRunFailed, JobsError, LimitReached
from pamfilico_jobs.run import Run


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class InlineRunner:
    def __init__(self, jobs):
        self.jobs = jobs
        self.store: dict[str, dict] = {}
        self.keys: dict[tuple, str] = {}

    def reset(self) -> None:
        self.store.clear()
        self.keys.clear()

    def _new(self, task, input, **meta) -> dict:
        run = {
            "id": str(uuid.uuid4()),
            "task": task,
            "app": task.split(".", 1)[0],
            "status": "queued",
            "input": input or {},
            "output": None,
            "error": None,
            "error_type": None,
            "attempt": 0,
            "max_attempts": 1,
            "trigger_kind": meta.pop("trigger_kind", "api"),
            "created_at": _now(),
            **meta,
        }
        self.store[run["id"]] = run
        return run

    def _execute(self, run: dict, parent_ctx=None) -> dict:
        tdef = self.jobs.registry.get(run["task"])
        if tdef is None:
            raise JobsError(404, "unknown_task", f"inline mode: no task {run['task']!r} is declared in this process")
        run.update(status="running", attempt=1, started_at=_now())
        ctx = Context(run)
        token = _ctx._current.set(ctx)
        try:
            result = tdef.call(run["input"], ctx)
            if asyncio.iscoroutine(result):
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    result = asyncio.run(result)
                else:  # pragma: no cover - called from a running loop: use run_child_async
                    raise RuntimeError("inline: an async task triggered from a running event loop; await jobs.run_child")
            run.update(status="done", output=result, finished_at=_now())
        except Exception as exc:  # noqa: BLE001
            run.update(
                status="failed",
                error="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                error_type=type(exc).__name__,
                finished_at=_now(),
            )
        finally:
            _ctx._current.reset(token)
        return run

    async def _execute_async(self, run: dict) -> dict:
        tdef = self.jobs.registry.get(run["task"])
        if tdef is None:
            raise JobsError(404, "unknown_task", f"inline mode: no task {run['task']!r} is declared in this process")
        run.update(status="running", attempt=1, started_at=_now())
        ctx = Context(run)
        token = _ctx._current.set(ctx)
        try:
            result = tdef.call(run["input"], ctx)
            if asyncio.iscoroutine(result):
                result = await result
            run.update(status="done", output=result, finished_at=_now())
        except Exception as exc:  # noqa: BLE001
            run.update(status="failed", error=str(exc), error_type=type(exc).__name__, finished_at=_now())
        finally:
            _ctx._current.reset(token)
        return run

    def trigger(self, task, input=None, *, client_id=None, subject=None, user_id=None, key=None, parent_run_id=None, trigger_kind=None, max_open=None):
        tdef = self.jobs.registry.get(task)
        if tdef is not None and tdef.require_key and key is None and not parent_run_id:
            raise JobsError(400, "key_required", f"{task} is not idempotent: pass an idempotency key")
        if key is not None and (task, key) in self.keys:
            existing = dict(self.store[self.keys[(task, key)]])
            existing["deduplicated"] = True
            return Run(existing)
        if max_open is not None:
            if client_id is None:
                raise ValueError("max_open needs a client_id: the limit is per client")
            # Inline runs finish inside trigger(), so "open" is only what is running right now
            # (a task triggering its own kind for the same client from inside itself).
            open_now = sum(1 for r in self.store.values()
                           if r.get("task") == task and r.get("client_id") == str(client_id)
                           and r.get("status") in ("queued", "running"))
            if open_now >= int(max_open):
                raise LimitReached(f"{task}: client {client_id} already has {open_now} open run(s) (max_open {max_open})")
        run = self._new(
            task, input, client_id=client_id, subject=subject, user_id=user_id, key=key,
            parent_run_id=parent_run_id, trigger_kind=trigger_kind or ("child" if parent_run_id else "api"),
        )
        if key is not None:
            self.keys[(task, key)] = run["id"]
        return Run(self._execute(run))

    def emit(self, name, payload=None, *, client_id=None, subject=None, user_id=None):
        runs = []
        for tdef in list(self.jobs.registry.tasks.values()):
            if any(fnmatch.fnmatchcase(name, p) for p in tdef.on_events):
                runs.append(
                    self.trigger(
                        tdef.name, {**(payload or {}), "_event": {"name": name}},
                        client_id=client_id, subject=subject, user_id=user_id, trigger_kind="event",
                    )
                )
        return runs

    def run_child_sync(self, task, input, ctx, **kw):
        run = self.trigger(task, input, parent_run_id=ctx.run_id, client_id=kw.get("client_id", ctx.client_id), subject=kw.get("subject"))
        if run.status != "done":
            raise ChildRunFailed(run)
        return run.output

    async def run_child_async(self, task, input, ctx, **kw):
        run = self._new(task, input, parent_run_id=ctx.run_id, client_id=kw.get("client_id", ctx.client_id), trigger_kind="child")
        run = await self._execute_async(run)
        if run["status"] != "done":
            raise ChildRunFailed(Run(run))
        return run["output"]

    def get(self, run_id):
        if run_id not in self.store:
            raise JobsError(404, "not_found", "run not found")
        return Run(self.store[run_id])

    def runs(self, **filters):
        out = []
        for run in reversed(list(self.store.values())):
            ok = True
            for k, v in filters.items():
                if k in ("limit", "offset") or v is None:
                    continue
                if k == "task" and "*" in str(v):
                    ok = fnmatch.fnmatchcase(run["task"], v)
                elif k == "status":
                    ok = run["status"] in str(v).split(",")
                else:
                    ok = run.get(k) == v
                if not ok:
                    break
            if ok:
                out.append(Run(run))
        return out[: int(filters.get("limit") or 50)]
