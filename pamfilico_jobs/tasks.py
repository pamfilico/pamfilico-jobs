from __future__ import annotations

import inspect
import logging
import re
from dataclasses import dataclass, field
from typing import Callable

from pamfilico_jobs.durations import seconds

logger = logging.getLogger("pamfilico_jobs.tasks")

TASK_NAME = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_][a-z0-9_\-]*)+$")
EVENT_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_\-*]+)+$")


@dataclass
class TaskDef:
    name: str
    fn: Callable
    #: A fixed schedule (``cron_string=``).
    cron: str | None = None
    #: A schedule read at every registration (``cron_callable=``): returns a cron string, or None
    #: for "no schedule". See ``current_cron``.
    cron_callable: Callable[[], str | None] | None = None
    #: The last cron string ``cron_callable`` returned that was valid (kept when it fails).
    _last_good_cron: str | None = None
    on_events: list = field(default_factory=list)
    fair_by: str | None = None
    one_open_per: str | None = None
    timeout_s: int = 600
    retries: int = 3
    backoff: list | None = None
    labels: dict = field(default_factory=dict)
    concurrency: int | None = None
    priority: int = 0
    description: str | None = None
    idempotent: bool = True
    require_key: bool = False
    retention_s: int | None = None
    queue_ttl_s: int | None = None

    @property
    def app(self) -> str:
        return self.name.split(".", 1)[0]

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    def call(self, inp, ctx):
        """Call the function with (input, ctx), (input) or () — whatever it accepts."""
        params = [
            p for p in inspect.signature(self.fn).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(p.kind == p.VAR_POSITIONAL for p in inspect.signature(self.fn).parameters.values())
        if has_varargs or len(params) >= 2:
            return self.fn(inp, ctx)
        if len(params) == 1:
            return self.fn(inp)
        return self.fn()

    def current_cron(self) -> str | None:
        """The schedule to register NOW.

        ``cron_string`` → that string, always. ``cron_callable`` → whatever it returns this time
        (the worker asks again at every registration, i.e. every minute). If the callable raises or
        returns something that is not a valid cron, the last good value is kept and a warning is
        logged, so a bad setting can never silently delete the schedule.
        """
        if self.cron_callable is None:
            return self.cron
        try:
            value = self.cron_callable()
        except Exception as exc:  # noqa: BLE001 — a broken setting must not take the worker down
            logger.warning("task %s: cron_callable raised %s; keeping %r", self.name, exc, self._last_good_cron)
            return self._last_good_cron
        if value is None:
            self._last_good_cron = None
            return None
        value = str(value).strip()
        if not _valid_cron(value):
            logger.warning("task %s: cron_callable returned %r, not a cron; keeping %r",
                           self.name, value, self._last_good_cron)
            return self._last_good_cron
        self._last_good_cron = value
        return value

    def definition(self) -> dict:
        return {
            "name": self.name,
            "cron": self.current_cron(),
            "on_events": list(self.on_events),
            "fair_by": self.fair_by,
            "one_open_per": self.one_open_per,
            "timeout_s": self.timeout_s,
            "retries": self.retries,
            "backoff": self.backoff,
            "labels": dict(self.labels),
            "concurrency": self.concurrency,
            "priority": self.priority,
            "description": self.description,
            "idempotent": self.idempotent,
            "require_key": self.require_key,
            "retention_s": self.retention_s,
            "queue_ttl_s": self.queue_ttl_s,
        }


class Registry:
    def __init__(self):
        self.tasks: dict[str, TaskDef] = {}

    def add(self, tdef: TaskDef) -> None:
        existing = self.tasks.get(tdef.name)
        if existing is not None and existing.fn is not tdef.fn and _qualname(existing.fn) != _qualname(tdef.fn):
            raise ValueError(f"task {tdef.name!r} is declared twice ({_qualname(existing.fn)} and {_qualname(tdef.fn)})")
        self.tasks[tdef.name] = tdef

    def get(self, name: str) -> TaskDef | None:
        return self.tasks.get(name)

    def for_app(self, app: str) -> list[TaskDef]:
        return [t for n, t in sorted(self.tasks.items()) if t.app == app]


def _valid_cron(value: str) -> bool:
    """True when ``value`` is a cron expression. Without croniter installed only the shape is checked
    (5 or 6 fields); the server validates it either way."""
    try:
        from croniter import croniter
    except ImportError:
        return len(value.split()) in (5, 6)
    return bool(croniter.is_valid(value))


def _qualname(fn) -> str:
    return f"{getattr(fn, '__module__', '?')}.{getattr(fn, '__qualname__', '?')}"


def make_task(
    name: str,
    fn: Callable,
    *,
    cron=None,
    cron_string=None,
    cron_callable=None,
    on_events=None,
    fair_by=None,
    one_open_per=None,
    timeout="10m",
    retries=None,
    backoff=None,
    labels=None,
    concurrency=None,
    priority=0,
    description=None,
    idempotent=True,
    require_key=None,
    retention=None,
    queue_ttl=None,
) -> TaskDef:
    if not TASK_NAME.match(name or ""):
        raise ValueError(f"bad task name {name!r}: use <app>.<domain>.<action>, lowercase")
    if one_open_per not in (None, "subject"):
        raise ValueError("one_open_per must be 'subject' or None")
    if fair_by not in (None, "client_id"):
        raise ValueError("fair_by must be 'client_id' or None")
    events = [on_events] if isinstance(on_events, str) else list(on_events or [])
    for e in events:
        if not EVENT_PATTERN.match(e):
            raise ValueError(f"bad event name {e!r}: use <app>.<entity>.<past-tense>")
    # `cron=` is the original name of `cron_string=` and still works.
    if cron is not None and cron_string is not None:
        raise ValueError("pass cron_string= (or its old name cron=), not both")
    cron_string = cron_string if cron_string is not None else cron
    if cron_string is not None and cron_callable is not None:
        raise ValueError("pass cron_string= OR cron_callable=, not both")
    if cron_callable is not None and not callable(cron_callable):
        raise ValueError("cron_callable must be a function that returns a cron string (or None)")
    if cron_string is not None and not _valid_cron(str(cron_string).strip()):
        raise ValueError(f"bad cron {cron_string!r}")
    timeout_s = int(seconds(timeout) or 600)
    if retries is None:
        # A task that costs money is not retried unless you say so.
        retries = 3 if idempotent else 0
    backoff_s = [int(seconds(b)) for b in backoff] if backoff else None
    doc = (description or (fn.__doc__ or "").strip().split("\n")[0] or None)
    return TaskDef(
        name=name,
        fn=fn,
        cron=str(cron_string).strip() if cron_string is not None else None,
        cron_callable=cron_callable,
        on_events=events,
        fair_by=fair_by,
        one_open_per=one_open_per,
        timeout_s=max(1, timeout_s),
        retries=max(0, int(retries)),
        backoff=backoff_s,
        labels={str(k): str(v) for k, v in (labels or {}).items()},
        concurrency=int(concurrency) if concurrency else None,
        priority=int(priority or 0),
        description=doc,
        idempotent=bool(idempotent),
        require_key=bool(require_key) if require_key is not None else not idempotent,
        retention_s=int(seconds(retention)) if retention else None,
        queue_ttl_s=int(seconds(queue_ttl)) if queue_ttl else None,
    )
