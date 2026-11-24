from __future__ import annotations

import json
import logging
import threading
import time

import requests

from pamfilico_jobs.errors import JobsError, JobsUnavailable, LimitReached

logger = logging.getLogger("pamfilico_jobs")


class Transport:
    """JSON over HTTP to the jobs API, with the app's key. One requests.Session per thread."""

    def __init__(self, base_url: str, api_key: str, user_agent: str, timeout: float = 10.0):
        if not base_url:
            raise JobsError(0, "not_configured", "JOBS_API_URL is not set (e.g. https://api.jobs.pamfili.co/api/v1)")
        if not api_key:
            raise JobsError(0, "not_configured", "JOBS_API_KEY is not set (a jb_… key from the jobs console)")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.user_agent = user_agent
        self.timeout = timeout
        self._local = threading.local()

    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update(
                {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "User-Agent": self.user_agent,
                }
            )
            self._local.session = s
        return s

    def request(self, method: str, path: str, *, json_body=None, params=None, timeout=None, attempts: int = 1):
        """Return the response's `data` (and `total` when present, as (data, total) if want_total)."""
        url = self.base_url + path
        last = None
        for i in range(max(1, attempts)):
            try:
                resp = self._session().request(
                    method,
                    url,
                    data=json.dumps(json_body, default=str) if json_body is not None else None,
                    params=params,
                    timeout=(5, timeout or self.timeout),
                )
            except requests.RequestException as exc:
                last = JobsUnavailable(f"{method} {path}: {type(exc).__name__}: {exc}")
            else:
                if resp.status_code >= 500 and resp.status_code not in (501,):
                    last = JobsUnavailable(f"{method} {path}: HTTP {resp.status_code}")
                else:
                    try:
                        payload = resp.json()
                    except ValueError:
                        payload = {"error": True, "code": "bad_response", "message": resp.text[:300]}
                    if resp.status_code == 409 and payload.get("code") == "limit_reached":
                        raise LimitReached(str(payload.get("message") or payload.get("ui_message") or "limit reached"))
                    if resp.status_code >= 400 or payload.get("error"):
                        raise JobsError(
                            resp.status_code,
                            payload.get("code") or "error",
                            payload.get("message") or payload.get("ui_message") or resp.text[:300],
                        )
                    return payload
            if i + 1 < attempts:
                time.sleep(min(2 ** i, 10))
        raise last
