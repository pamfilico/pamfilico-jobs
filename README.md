<p align="center"><img src="logo.svg" alt="" width="72"></p>

# pamfilico-jobs

The Python SDK for **jobs**, a central job plane for many apps. Declare tasks next to your code,
start runs from anywhere, run a worker inside your own app, and follow every run (retries, failures,
children in other apps) in one console.

**The one idea:** jobs keeps the list of work. **Your app's worker does the work**, with your code,
your dependencies and your database. jobs never runs your code and never stores your business data.

```bash
poetry add git+https://github.com/pamfilico/pamfilico-jobs.git#v0.2.3
# or: pip install "pamfilico-jobs @ git+https://github.com/pamfilico/pamfilico-jobs.git@v0.2.3"
```

Python 3.10+. One dependency (`requests`); `croniter` is optional (validates crons locally).

---

## Contents

1. [Quick start](#1-quick-start)
2. [Declaring tasks](#2-declaring-tasks)
3. [Inside a task: `ctx`](#3-inside-a-task-ctx)
4. [Starting and reading runs](#4-starting-and-reading-runs)
5. [Children and events: work across apps](#5-children-and-events-work-across-apps)
6. [Idempotency: which tasks may run twice](#6-idempotency-which-tasks-may-run-twice)
7. [Time to live](#7-time-to-live)
8. [Traces](#8-traces)
9. [The worker](#9-the-worker)
10. [Webhooks](#10-webhooks)
11. [Testing](#11-testing)
12. [Errors](#12-errors)
13. [Configuration](#13-configuration)

---

## 1. Quick start

```python
# app/domains/reports/jobs.py
from pamfilico_jobs import jobs

@jobs.task("core.reports.build", timeout="5m")
def build(inp, ctx):
    ctx.progress(30, "loading")
    report_id = Reports.build(inp["month"])           # your code, your database
    return {"report_id": report_id}                   # a small reference, not the data
```

```python
# anywhere in the app (an endpoint, another task)
run = jobs.trigger("core.reports.build", {"month": "2026-11"}, client_id="c1")
run = jobs.wait(run.id, timeout=60)
print(run.status, run.output)                         # done {'report_id': 42}
```

```python
# worker_jobs.py: one process, one compose service, same image as your backend
from pamfilico_jobs import jobs
jobs.worker("core", slots=10).discover("app.domains").start()
```

```bash
export JOBS_API_URL=https://api.jobs.example.com/api/v1
export JOBS_API_KEY=jb_…          # from the jobs console: Apps & keys
python worker_jobs.py
```

The app named `core` runs every `core.*` task. Task names are `<app>.<domain>.<action>`.

## 2. Declaring tasks

`@jobs.task` registers the function and returns it **unchanged**: call it directly in unit tests.
The function takes `(inp, ctx)`, `(inp)` or nothing, and may be `async`.

```python
@jobs.task(
    "core.signals.run",
    timeout="5m",              # per attempt
    retries=3,                 # attempts after the first (default 3; 0 when idempotent=False)
    backoff=["10s", "1m"],     # wait before each retry; the last value repeats
    fair_by="client_id",       # one client's backlog can't starve the others
    one_open_per="subject",    # a trigger while one is open returns the open run
    concurrency=5,             # at most 5 running at once, across every worker
    labels={"gpu": "true"},    # only workers with these labels take it
    priority=0,                # higher first
    cron=None,                 # "*/5 * * * *"; a 6th field is seconds
    on_events=None,            # ["crawlfast.crawl.*"]: start a run per matching event
    idempotent=True,           # False = running it twice has a cost (§6)
    require_key=None,          # refuse triggers without a key (default: not idempotent)
    retention=None,            # keep the finished run's record this long ("7d"; §7)
    queue_ttl=None,            # expire it if no worker took it in time ("10m"; §7)
)
def run(inp, ctx): ...
```

| Option | Default | Notes |
|---|---|---|
| `timeout` | `"10m"` | the attempt fails with `timeout`; a late result is dropped |
| `retries` | `3` (`0` if not idempotent) | backoff default 10 s, 1 m, 5 m, 15 m, 1 h |
| `cron` | — | ticks never overlap: a slow tick makes the next one a no-op. Switch crons off in the console |
| `fair_by` | — | claiming is round robin between clients |
| `one_open_per` | — | `"subject"` only |
| `concurrency` | — | global per task: protect a shared API or a fragile service |
| `labels` | — | route work to machines (`{"gpu": "true"}`) |

One function can serve two task names with different policies:

```python
@jobs.task("crawlfast.serper.fetch")
@jobs.task("crawlfast.serper.fetch_leads", concurrency=2)   # the lead lane gets fewer slots
def fetch(inp, ctx): ...
```

## 3. Inside a task: `ctx`

| | |
|---|---|
| `ctx.progress(40, "scored 40/100")` | live progress in the console (sent with the heartbeat) |
| `ctx.attempt`, `ctx.max_attempts`, `ctx.is_last_attempt` | where you are in the retries |
| `ctx.run_id`, `ctx.task`, `ctx.trace_id` | this run |
| `ctx.client_id`, `ctx.subject`, `ctx.user_id`, `ctx.key` | what it is about |
| `ctx.parent_run_id`, `ctx.source_run_id`, `ctx.trigger_kind` | where it came from |
| `ctx.cancelled()` / `ctx.raise_if_cancelled()` | check in long loops; true after a cancel, or when jobs took the run back |
| `ctx.lost` | jobs took the run back (lease expired / timed out); your result will be ignored |
| `raise NonRetryable("…")` | fail now, without the remaining retries |

## 4. Starting and reading runs

```python
run = jobs.trigger(
    "core.reports.build", {"month": "2026-11"},
    client_id="c1",                 # who it is for (filters, fairness)
    subject="report:c1:2026-11",    # what it is about (filters, one_open_per)
    user_id="u7",
    key="report:c1:2026-11",        # idempotency: same key → the same run back
    priority=5,
    delay="10m",                    # or run_at=datetime(...)
    trace_id=None,                  # continue a trace from outside a task (§8)
)
run.id, run.status, run.deduplicated

jobs.get(run.id)                                   # Run: status, attempt, output, error, timings …
jobs.wait(run.id, timeout=60)                      # long-polls; raise_on_failure=True raises RunFailed
jobs.runs(task="core.reports.*", status="failed", client_id="c1", limit=50)
jobs.logs(run.id)                                  # queued, claimed, attempt_failed, retry_scheduled, done …
jobs.cancel(run.id)                                # queued: now; running: the task sees ctx.cancelled()
jobs.purge(task="core.signals.run", client_id="c1")  # cancel every QUEUED run that matches; returns the count
jobs.retry(run.id)                                 # a NEW run with retry_of = the old one
```

A `Run` exposes the JSON as attributes: `id, task, app, status, input, output, error, error_type,
attempt, max_attempts, client_id, subject, key, dedup_hits, trace_id, parent_run_id,
source_run_id, created_at, started_at, finished_at, duration_s, expires_at, needs_review, …`
(`run.raw` is the dict). `run.finished`, `run.ok`.

Statuses: `queued`, `running`, `done`, `failed`, `cancelled`. `error_type` says why: the exception
class, `timeout`, `lease_expired`, `cancelled`, `expired` (queue TTL), `child_failed`,
`outcome_unknown` (§6), `OutputTooLarge`.

## 5. Children and events: work across apps

A task can hand part of its work to **another app's** worker and wait for the result:

```python
@jobs.task("core.leads.score", one_open_per="subject")
async def score(inp, ctx):
    lead = Leads.get(inp["lead_id"])                                   # core's database
    result = await jobs.run_child("ai.signals.assess", {"text": lead.text})   # ai's worker, ai's database
    Leads.set_score(lead.id, result["score"])                          # back in core
    return {"score": result["score"]}
```

- `await jobs.run_child(task, input)` / `await jobs.run_children(task, [inputs])` in async tasks;
  `jobs.run_child_sync` / `jobs.run_children_sync` in plain ones. They return the child's output
  and raise `ChildRunFailed` if it failed (retryable by default).
- **A retried parent gets the same child back:** the child's key comes from (parent, task, input),
  so a finished child is not run again and a failed one is re-queued.
- The parent keeps its worker slot while it waits: give workers enough `slots`.

Events start every task that subscribed, in any app:

```python
@jobs.task("core.leads.on_assessed", on_events=["ai.assessment.recorded"])
def on_assessed(inp, ctx): ...                       # inp = the payload + inp["_event"]

jobs.emit("ai.assessment.recorded", {"lead_id": 7, "score": 0.8}, client_id="c1")   # → [Run, …]
```

## 6. Idempotency: which tasks may run twice

A run can be handed out twice: a worker freezes, its lease expires, another worker takes it. So
decide, per task, what running it twice means.

**Most tasks are idempotent (the default)**: recompute a value, import results. Write with upserts,
check before side effects, and jobs retries freely, including after a lost worker or a timeout.

**Some cost money or can't be undone** (a charge, an email, a paid API call, a long agent run):

```python
@jobs.task("core.billing.charge", idempotent=False, timeout="30s", retention="400d")
def charge(inp, ctx):
    provider.charge(inp["order_id"], idempotency_key=ctx.key)     # pass the key on if the provider takes one

jobs.trigger("core.billing.charge", {"order_id": 7}, key="charge:7")   # a key is required
jobs.trigger("core.billing.charge", {"order_id": 7}, key="charge:7")   # → the same run: charged once
```

| | idempotent (default) | `idempotent=False` |
|---|---|---|
| trigger without a key | allowed | refused (`key_required`) |
| same key again | the same run (`deduplicated`) | the same |
| an exception | retried | retried only if you set `retries` |
| worker lost / timed out mid-run | retried | **not retried**: `failed` / `outcome_unknown`, `run.needs_review` |

An `outcome_unknown` run waits in the console under **Needs review** until someone checks the outside
system and marks it reviewed, or runs it again (with a confirmation).

## 7. Time to live

- **`queue_ttl`**: a run nobody claimed in time is cancelled with `error_type="expired"` instead of
  running late. Use it for ticks and for work someone is waiting for live.
- **`retention`**: how long the finished run's record is kept (`run.expires_at`). Default: the
  server's (30 days). Longer for billing, shorter for noise. Deleting a run record never touches
  your app's data.

## 8. Traces

Every run has a `trace_id`. The first run of an action starts the trace, and everything it causes
joins it, in every app: its children, runs it triggered or started by emitting events, and
"run again"s. Inside a task this is automatic.

```python
trace = jobs.trace(run.id)          # any run id in it, or the trace id
trace["runs"]                       # every run with its history; other apps' runs as stubs
trace["events"], trace["deliveries"]
```

A flow that waits for a person continues its trace from the web request that resumes it:

```python
Briefs.save(draft, trace_id=ctx.trace_id)                       # inside the first task
jobs.emit("core.brief.approved", {"brief_id": b.id}, trace_id=b.trace_id)   # in the Approve endpoint
```

## 9. The worker

```python
jobs.worker("core", slots=10, labels={"gpu": "true"}, name="core@box1") \
    .discover("app.domains") \
    .start()
```

- `discover("pkg")` imports every module named `jobs` under the package; `.include("app.x.jobs")`
  imports one; `.tasks("core.a.b", …)` limits this process to some tasks (a dedicated worker for
  slow or GPU work).
- It **connects out** (HTTPS), so it runs anywhere: in your compose stack, on a laptop, on a GPU box
  at home.
- It registers its tasks (and crons, events, policies) at start and every minute; long-polls for
  work; runs each run in a thread (async tasks get their own loop); heartbeats while it runs.
- `SIGTERM` / `SIGINT`: stop claiming, let running tasks finish (`JOBS_SHUTDOWN_GRACE_SECONDS`,
  default 30), deregister. Anything still running is reclaimed by its lease. Give the container a
  `stop_grace_period` longer than that.
- jobs unreachable: it backs off and keeps trying; nothing is lost.

## 10. Webhooks

jobs can POST a signed message to your app when runs change (`run.queued`, `run.started`,
`run.retrying`, `run.completed`, `run.failed`, `run.cancelled`), retrying until it gets a 2xx.

```python
hook = jobs.create_webhook("http://backend:5000/internal/jobs/events",
                           events=["run.completed", "run.failed"], task_filter="core.*")
hook["secret"]                      # whsec_…: shown ONCE, store it
jobs.webhooks(); jobs.delete_webhook(hook["id"])
```

```python
from pamfilico_jobs.webhooks import verify   # HMAC-SHA256 + a 5-minute timestamp window

@app.post("/internal/jobs/events")
def jobs_events():
    event = verify(request.get_data(), request.headers, secret=os.environ["JOBS_WEBHOOK_SECRET"])
    # event["type"], event["sequence"], event["data"] (the run)
    ...
    return "", 204
```

Rules: a message can arrive twice (dedupe on `X-Jobs-Delivery`), order is not guaranteed (keep the
highest `sequence` per run), answer fast and do heavy work afterwards. If several processes of your
app register at startup, take a lock first or you get one webhook per process. Full example:
[`examples/webhook_receiver.py`](examples/webhook_receiver.py).

## 11. Testing

```python
from pamfilico_jobs.testing import inline

def test_report():
    with inline():                                   # trigger() runs the task right here
        run = jobs.trigger("core.reports.build", {"month": "2026-11"})
        assert run.status == "done" and run.output["report_id"]
```

Or `PAMFILICO_JOBS_MODE=inline pytest`. Inline mode runs tasks, children and event subscribers
once, keeps idempotency keys and refuses a non-idempotent trigger without a key. Retries, timeouts,
crons, workers and webhooks are the engine's job. Task functions are plain functions: call them
directly too.

## 12. Errors

| Exception | When |
|---|---|
| `JobsError(status, code, message)` | jobs said no: `unauthorized`, `scope`, `unknown_app`, `bad_task_name`, `key_required`, `not_found`, `too_large`, … |
| `JobsUnavailable` | network error, timeout, 5xx (a `JobsError` with code `unavailable`) |
| `NonRetryable` | raise it in a task to fail without retries |
| `Cancelled` | raised by `ctx.raise_if_cancelled()` |
| `RunFailed` / `ChildRunFailed` | a run you waited for did not finish `done` (`.run` has it) |
| `WaitTimeout` | `wait()` / a child ran past its timeout (`.run`) |
| `InvalidSignature` | `webhooks.verify` refused a message |

Limits: input and event payloads 256 KB, output 512 KB. Store data in your app and pass a reference.

## 13. Configuration

| Variable | |
|---|---|
| `JOBS_API_URL` | e.g. `https://api.jobs.example.com/api/v1` |
| `JOBS_API_KEY` | `jb_…`, one per app, from the console. Scopes: `trigger`, `work`, `read` (`admin` sees every app) |
| `PAMFILICO_JOBS_MODE` | `inline` in tests; default `remote` |
| `JOBS_WORKER_NAME` | the worker's display name |
| `JOBS_SHUTDOWN_GRACE_SECONDS` | default 30 |

Or explicitly: `Jobs(url=…, key=…)` / `jobs.configure(url=…, key=…, mode=…)`. Never let a local
worker use a production key: it would take production runs.

---

Changelog: [`CHANGELOG.md`](CHANGELOG.md). Examples: [`examples/`](examples/). MIT licensed.
