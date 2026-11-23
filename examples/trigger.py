"""Start work from anywhere in the app, then read it back."""

from pamfilico_jobs import jobs

run = jobs.trigger("shop.reports.build", {"month": "2026-11"}, client_id="c1", subject="report:2026-11")
run = jobs.wait(run.id, timeout=60)
print(run.status, run.output, run.attempt)

charge = jobs.trigger("shop.billing.charge", {"order_id": 7, "amount": 1200}, key="charge:7")
again = jobs.trigger("shop.billing.charge", {"order_id": 7, "amount": 1200}, key="charge:7")
assert again.id == charge.id and again.deduplicated     # charged once

for r in jobs.trace(run.id)["runs"]:
    print(r["task"], r["status"])
