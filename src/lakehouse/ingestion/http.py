"""HTTP GET with retries (exponential backoff on 429/5xx/network errors) and a per-host rate limit."""

import threading
import time

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


class RetryableHTTPError(Exception):
    pass


def _retryable(exc: BaseException) -> bool:
    return isinstance(exc, RetryableHTTPError | requests.ConnectionError | requests.Timeout)


class RateLimiter:
    def __init__(self, per_second: float):
        self.min_interval = 1.0 / per_second
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            delay = self._last + self.min_interval - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._last = time.monotonic()


@retry(retry=retry_if_exception(_retryable), wait=wait_exponential(multiplier=1, max=30),
       stop=stop_after_attempt(5), reraise=True)
def get(url: str, *, limiter: RateLimiter, headers: dict, params: dict | None = None,
        timeout: float = 60) -> requests.Response:
    limiter.wait()
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableHTTPError(f"HTTP {resp.status_code} from {url.split('?')[0]}")
    resp.raise_for_status()
    return resp
