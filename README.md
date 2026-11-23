# pamfilico-jobs

Python SDK for **jobs**, the central job plane: declare tasks next to your code, start runs from
anywhere, run a worker in your own app, verify jobs' webhooks. jobs keeps the list of work; your
worker does the work, with your code and your database.

```bash
poetry add git+https://github.com/pamfilico/pamfilico-jobs.git#v0.2.1
```

```python
from pamfilico_jobs import jobs, NonRetryable

@jobs.task("core.reports.build", fair_by="client_id", one_open_per="subject", timeout="5m", retries=3)
def build(inp, ctx):
    ctx.progress(10, "loading")
    if not inp.get("month"):
        raise NonRetryable("month is required")       # fail now, no retries
    return {"report_id": 42}                           # small JSON: a reference, not the data

@jobs.task("core.reports.tick", cron="*/5 * * * *", queue_ttl="4m")   # a cron; ticks never overlap, stale ones expire

@jobs.task("core.billing.charge", idempotent=False, retention="400d")  # costs money: key required,
def charge(inp, ctx): ...                                                # never auto-retried when the outcome is unknown
def tick(inp, ctx): ...

@jobs.task("core.parent.go")
async def parent(inp, ctx):
    score = await jobs.run_child("ai.signals.assess", {"lead_id": inp["lead_id"]})  # another app's worker
    return {"score": score}

run = jobs.trigger("core.reports.build", {"month": "2026-11"}, client_id="c1", subject="report:c1:2026-11")
run = jobs.wait(run.id, timeout=60)        # run.status, run.output, run.error, run.attempt
jobs.emit("core.lead.crawled", {"lead_id": 7}, client_id="c1")
jobs.runs(task="core.reports.*", status="failed", client_id="c1")
jobs.cancel(run.id); jobs.retry(run.id)
jobs.trigger("core.billing.charge", {"order_id": 7}, key="charge:7")   # same key → same run
jobs.trace(run.id)                         # everything this action caused, across apps
```

Worker (one process; `slots` runs at once):

```python
# worker_jobs.py
from pamfilico_jobs import jobs
jobs.worker("core", slots=10).discover("app.domains").start()   # imports every */jobs.py
```

Environment: `JOBS_API_URL` (e.g. `https://api.jobs.pamfili.co/api/v1`), `JOBS_API_KEY` (`jb_…`),
`PAMFILICO_JOBS_MODE=inline` in tests (trigger runs the task in-process).

Webhooks:

```python
from pamfilico_jobs.webhooks import verify
event = verify(request.get_data(), request.headers, secret=os.environ["JOBS_WEBHOOK_SECRET"])
```

The full guide is `docs/INTEGRATING.md` in the jobs repo. The source of this package lives in the
jobs repo at `sdk/python`; this GitHub repo is its published copy.

MIT licensed.
