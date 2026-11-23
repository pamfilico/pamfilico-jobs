import asyncio
import json
import time

import pytest

from pamfilico_jobs import ChildRunFailed, Jobs, NonRetryable
from pamfilico_jobs.durations import seconds
from pamfilico_jobs.errors import InvalidSignature
from pamfilico_jobs.webhooks import sign, verify


def make():
    j = Jobs(mode="inline")
    return j


def test_durations():
    assert seconds("90s") == 90
    assert seconds("10m") == 600
    assert seconds("1h30m") == 5400
    assert seconds(5) == 5
    assert seconds(None) is None
    with pytest.raises(ValueError):
        seconds("ten minutes")


def test_task_name_and_options_validated():
    j = make()
    with pytest.raises(ValueError):
        j.task("Bad Name")(lambda i, c: None)
    with pytest.raises(ValueError):
        j.task("core.x.y", one_open_per="client")(lambda i, c: None)
    with pytest.raises(ValueError):
        j.task("core.x.y", on_events=["Not Valid"])(lambda i, c: None)


def test_decorated_function_stays_plain():
    j = make()

    @j.task("core.math.add", timeout="30s", retries=1)
    def add(inp, ctx):
        return {"sum": inp["a"] + inp["b"]}

    assert add({"a": 1, "b": 2}, None) == {"sum": 3}
    assert add.task.timeout_s == 30 and add.task.retries == 1
    assert add.task.definition()["name"] == "core.math.add"


def test_inline_trigger_done_and_failed():
    j = make()

    @j.task("core.math.add")
    def add(inp, ctx):
        assert ctx.run_id and ctx.attempt == 1
        return {"sum": inp["a"] + inp["b"]}

    @j.task("core.math.boom")
    def boom(inp):
        raise NonRetryable("nope")

    ok = j.trigger("core.math.add", {"a": 2, "b": 3}, client_id=7)
    assert ok.status == "done" and ok.output == {"sum": 5} and ok.client_id == "7" or ok.client_id == 7
    bad = j.trigger("core.math.boom", {})
    assert bad.status == "failed" and bad.error_type == "NonRetryable"
    assert j.get(ok.id).status == "done"
    assert [r.id for r in j.runs(status="failed")] == [bad.id]


def test_inline_idempotency_key():
    j = make()
    calls = []

    @j.task("core.once.do")
    def do(inp, ctx):
        calls.append(1)
        return {"n": len(calls)}

    a = j.trigger("core.once.do", {}, key="k1")
    b = j.trigger("core.once.do", {}, key="k1")
    assert a.id == b.id and b.deduplicated and len(calls) == 1


def test_inline_children_sync_and_async():
    j = make()

    @j.task("ai.score.one")
    def score(inp, ctx):
        if inp["x"] < 0:
            raise ValueError("negative")
        return inp["x"] * 10

    @j.task("core.parent.sync")
    def parent(inp, ctx):
        return j.run_child_sync("ai.score.one", {"x": inp["x"]})

    @j.task("core.parent.async")
    async def parent_async(inp, ctx):
        outs = await j.run_children("ai.score.one", [{"x": 1}, {"x": 2}])
        return sum(outs)

    assert j.trigger("core.parent.sync", {"x": 4}).output == 40
    assert j.trigger("core.parent.async", {}).output == 30
    failed = j.trigger("core.parent.sync", {"x": -1})
    assert failed.status == "failed" and failed.error_type == "ChildRunFailed"


def test_inline_events():
    j = make()

    @j.task("ai.lead.enrich", on_events=["core.lead.*"])
    def enrich(inp, ctx):
        return {"lead": inp["lead_id"], "event": inp["_event"]["name"]}

    runs = j.emit("core.lead.crawled", {"lead_id": 9})
    assert len(runs) == 1 and runs[0].output == {"lead": 9, "event": "core.lead.crawled"}
    assert j.emit("core.other.thing", {}) == []


def test_run_child_outside_task_is_an_error():
    j = make()
    with pytest.raises(RuntimeError):
        j.run_child_sync("ai.x.y", {})


def test_webhook_verify_roundtrip_and_tamper():
    secret = "whsec_test"
    body = json.dumps({"type": "run.completed", "data": {"id": "r1"}}).encode()
    headers = {"X-Jobs-Signature": sign(secret, body)}
    assert verify(body, headers, secret)["type"] == "run.completed"
    with pytest.raises(InvalidSignature):
        verify(body + b" ", headers, secret)
    with pytest.raises(InvalidSignature):
        verify(body, headers, "whsec_other")
    old = {"x-jobs-signature": sign(secret, body, timestamp=int(time.time()) - 3600)}
    with pytest.raises(InvalidSignature):
        verify(body, old, secret)


def test_remote_mode_needs_configuration(monkeypatch):
    monkeypatch.delenv("JOBS_API_URL", raising=False)
    monkeypatch.delenv("JOBS_API_KEY", raising=False)
    j = Jobs(mode="remote")
    from pamfilico_jobs import JobsError

    with pytest.raises(JobsError) as e:
        j.trigger("core.x.y", {})
    assert e.value.code == "not_configured"


def test_money_tasks_default_to_no_retries_and_a_required_key():
    j = make()

    @j.task("core.billing.charge", idempotent=False, retention="90d", queue_ttl="10m")
    def charge(inp, ctx):
        return {}

    d = charge.task.definition()
    assert d["idempotent"] is False and d["require_key"] is True and d["retries"] == 0
    assert d["retention_s"] == 90 * 86400 and d["queue_ttl_s"] == 600

    @j.task("core.billing.refund", idempotent=False, retries=2, require_key=False)
    def refund(inp, ctx):
        return {}

    assert refund.task.retries == 2 and refund.task.require_key is False

    @j.task("core.math.safe")
    def safe(inp, ctx):
        return {}

    assert safe.task.idempotent and safe.task.retries == 3 and not safe.task.require_key


def test_one_function_can_serve_two_task_names_with_different_policies():
    j = make()

    @j.task("crawlfast.serper.fetch", retries=3)
    @j.task("crawlfast.serper.fetch_leads", retries=3, concurrency=2)
    def fetch(inp, ctx):
        return {"q": inp["q"]}

    assert j.registry.get("crawlfast.serper.fetch").concurrency is None
    assert j.registry.get("crawlfast.serper.fetch_leads").concurrency == 2
    assert j.trigger("crawlfast.serper.fetch_leads", {"q": "x"}).output == {"q": "x"}
    assert j.trigger("crawlfast.serper.fetch", {"q": "y"}).output == {"q": "y"}
