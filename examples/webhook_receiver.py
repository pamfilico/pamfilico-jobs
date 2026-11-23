"""A Flask endpoint that receives jobs' webhooks: verify, dedupe, keep the newest, answer fast."""

import os

from flask import Flask, request
from pamfilico_jobs.errors import InvalidSignature
from pamfilico_jobs.webhooks import verify

app = Flask(__name__)
SEEN: set[str] = set()                 # use your database in an app (a unique delivery id column)
LATEST: dict[str, int] = {}            # run id → highest sequence seen


@app.post("/internal/jobs/events")
def jobs_events():
    try:
        event = verify(request.get_data(), request.headers, secret=os.environ["JOBS_WEBHOOK_SECRET"])
    except InvalidSignature:
        return {"error": "bad signature"}, 401
    if event["id"] in SEEN:            # at least once: the same message may arrive twice
        return "", 204
    SEEN.add(event["id"])
    run = event["data"]
    if event["sequence"] < LATEST.get(run["id"], -1):   # no ordering guarantee
        return "", 204
    LATEST[run["id"]] = event["sequence"]
    print(event["type"], run["task"], run["status"])     # hand heavy work to your own queue here
    return "", 204
