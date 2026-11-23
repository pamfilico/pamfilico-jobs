"""Test helpers.

    from pamfilico_jobs.testing import inline

    def test_signal_runs():
        with inline():                       # trigger() runs the task here, no jobs needed
            run = jobs.trigger("core.signals.run", {"lead_id": 1})
            assert run.status == "done"
"""

from __future__ import annotations

import contextlib


@contextlib.contextmanager
def inline(client=None):
    from pamfilico_jobs import jobs as default

    client = client or default
    previous = client._mode
    client.configure(mode="inline")
    client._inline_runner().reset()
    try:
        yield client
    finally:
        client._mode = previous
        client._inline_runner().reset()
