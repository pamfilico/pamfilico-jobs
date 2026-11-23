"""Tasks for an app named `shop`. Import this module from the worker (see worker.py)."""

import time

from pamfilico_jobs import NonRetryable, jobs


@jobs.task("shop.reports.build", timeout="5m", one_open_per="subject")
def build_report(inp, ctx):
    """Idempotent: rebuilding the same month overwrites the same report."""
    if not inp.get("month"):
        raise NonRetryable("month is required")
    for step in range(1, 4):
        ctx.raise_if_cancelled()
        ctx.progress(step * 33, f"step {step}/3")
        time.sleep(0.5)
    return {"report": f"report-{inp['month']}"}          # a reference, not the data


@jobs.task("shop.reports.nightly", cron="0 2 * * *", queue_ttl="1h")
def nightly(inp, ctx):
    """A cron. Runs it starts join this run's trace."""
    run = jobs.trigger("shop.reports.build", {"month": inp["scheduled_for"][:7]},
                       subject=f"report:{inp['scheduled_for'][:7]}")
    return {"started": run.id}


@jobs.task("shop.billing.charge", idempotent=False, timeout="30s", retention="400d")
def charge(inp, ctx):
    """Costs money: triggers need a key, and a lost attempt waits for review instead of retrying."""
    # payment_provider.charge(inp["order_id"], inp["amount"], idempotency_key=ctx.key)
    return {"charged": inp["order_id"]}


@jobs.task("shop.orders.fulfil")
async def fulfil(inp, ctx):
    """Hands part of the work to another app's worker and waits for it."""
    label = await jobs.run_child("warehouse.labels.print", {"order_id": inp["order_id"]})
    return {"label": label}
