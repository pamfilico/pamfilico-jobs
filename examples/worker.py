"""Run: JOBS_API_URL=… JOBS_API_KEY=jb_… python examples/worker.py"""

import logging

from pamfilico_jobs import jobs

import tasks  # noqa: F401  (registers shop.* tasks; in an app: .discover("app.domains"))

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    jobs.worker("shop", slots=4).start()
