# Changelog

## 0.2.4
- `cron_string=`: the fixed schedule (`cron=` is its old name and still works).
- `cron_callable=`: a function returning the schedule (a cron string, or `None` for none). The worker
  calls it at every registration (every minute), so a schedule can change without a deploy. If it
  raises or returns an invalid cron, the last good schedule is kept and a warning is logged.
- The two are exclusive: passing both (or `cron` with `cron_string`) raises at import time.

## 0.2.3
- `purge(task=, client_id=, subject=)`: cancel this app's queued runs in one call (`*` allowed in
  `task` and `subject`). Running runs are left alone; returns how many were cancelled.

## 0.2.2
- README rewritten as a complete reference (every option, `ctx`, errors, configuration).
- `py.typed`: the package ships its type hints.
- `examples/`: a task module, a worker, a webhook receiver, a money task.

## 0.2.1
- `trigger(..., trace_id=)` and `emit(..., trace_id=)`: continue a trace from outside a task (a web
  request that resumes a flow, e.g. an approval).

## 0.2.0
- Per-task idempotency: `idempotent=False` (a key is required, retries default to 0, a lost or
  timed-out attempt is held as `outcome_unknown` for review instead of retried) and `require_key`.
- Time to live: `retention` (how long the finished run's record is kept) and `queue_ttl` (expire a
  run nobody claimed in time).
- Traces: runs triggered and events emitted inside a task join its trace automatically;
  `jobs.trace(run_id)`; `ctx.trace_id`, `ctx.source_run_id`.
- Inline mode refuses a non-idempotent trigger without a key.

## 0.1.0
- `@jobs.task` (timeout, retries, backoff, cron, on_events, fair_by, one_open_per, concurrency,
  labels, priority), `trigger` / `emit` / `get` / `runs` / `wait` / `cancel` / `retry` / `logs`,
  `run_child(_sync)` / `run_children(_sync)`, the worker (discover, labels, heartbeats, graceful
  stop), inline test mode, `webhooks.verify`.
