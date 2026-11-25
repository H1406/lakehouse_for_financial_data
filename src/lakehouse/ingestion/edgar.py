"""SEC EDGAR: submissions (to detect new filings) and companyfacts (full XBRL payload)."""

import os

from pydantic import ValidationError

from lakehouse.config import settings
from lakehouse.ingestion import http
from lakehouse.ingestion.contracts import CompanyFactsEnvelope

BASE_URL = os.getenv("SEC_BASE_URL", "https://data.sec.gov")
PERIODIC_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
_limiter = http.RateLimiter(per_second=8)  # SEC fair-access limit is 10 req/s


def _headers() -> dict:
    ua = settings().sec_user_agent
    if not ua:
        raise RuntimeError("SEC_USER_AGENT is not set (SEC requires a descriptive User-Agent with contact email)")
    return {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}


def cik10(cik: str | int) -> str:
    return str(int(cik)).zfill(10)


def fetch_submissions(cik: str) -> dict:
    return http.get(f"{BASE_URL}/submissions/CIK{cik10(cik)}.json", limiter=_limiter, headers=_headers()).json()


def fetch_companyfacts(cik: str) -> str:
    return http.get(f"{BASE_URL}/api/xbrl/companyfacts/CIK{cik10(cik)}.json",
                    limiter=_limiter, headers=_headers()).text


def latest_periodic_filing_date(submissions: dict) -> str | None:
    """Most recent 10-K/10-Q (incl. amendments) filingDate in `filings.recent`, as YYYY-MM-DD."""
    recent = submissions["filings"]["recent"]
    dates = [d for f, d in zip(recent["form"], recent["filingDate"], strict=True) if f in PERIODIC_FORMS]
    return max(dates) if dates else None


def validate_companyfacts(payload: str) -> str | None:
    """None if the payload passes the envelope contract, else a reason for quarantine."""
    try:
        CompanyFactsEnvelope.model_validate_json(payload)
    except ValidationError as exc:
        return "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())[:500]
    return None
