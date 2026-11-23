# Changelog

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
