"""PII separation (stretch). Synthetic alert subscribers; contact details never leave the `pii` schema.

- pii.subscribers (name, email, phone)            readable only by role pii_admin
- lake_silver.alert_subscriptions(subscriber_key)  subscriber_key = HMAC-SHA256(secret, subscriber_id): no PII,
  and without the secret the key cannot be linked back (unlike a plain hash of a small id space).
- marts.fct_alert_targets (dbt)                    which subscriber_keys to alert today, still no PII
- notify(): the only code path that joins back to contacts, running as pii_admin; logs masked emails only.

    python -m agents.pii.subscribers seed      # create 40 synthetic subscribers (idempotent)
    python -m agents.pii.subscribers notify    # dry run: who would be alerted today
"""

import hashlib
import hmac
import random
import sys

import psycopg

from lakehouse.config import settings

FIRST = ["An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Khanh", "Linh", "Minh", "Nam", "Phuong", "Quang", "Thao",
         "Trang", "Tuan", "Vy", "Alex", "Sam", "Jordan", "Taylor"]
LAST = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ngo", "Smith", "Garcia"]
TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "JPM", "V", "XOM", "JNJ", "WMT", "KO", "BTCUSDT"]


def subscriber_key(subscriber_id: int) -> str:
    secret = settings().pii_hmac_secret.get_secret_value().encode()
    return hmac.new(secret, str(subscriber_id).encode(), hashlib.sha256).hexdigest()


def mask_email(email: str) -> str:
    user, _, domain = email.partition("@")
    return f"{user[0]}***@{domain}"


def seed(n: int = 40) -> dict:
    rng = random.Random(42)  # deterministic synthetic data
    people = []
    for i in range(1, n + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        people.append((i, f"{first} {last}", f"{first.lower()}.{last.lower()}{i}@example.com",
                       f"+84 9{rng.randint(10, 99)} {rng.randint(100, 999)} {rng.randint(100, 999)}"))
    subs = {(subscriber_key(i), t) for i, *_ in people for t in rng.sample(TICKERS, rng.randint(1, 3))}
    with psycopg.connect(settings().dsn_for("pii_admin"), autocommit=True) as conn:
        conn.cursor().executemany(
            "insert into pii.subscribers (subscriber_id, name, email, phone) values (%s, %s, %s, %s) "
            "on conflict (subscriber_id) do nothing", people)
    with psycopg.connect(settings().pg_dsn, autocommit=True) as conn:  # etl_writer: keys only, never contacts
        conn.cursor().executemany(
            "insert into lake_silver.alert_subscriptions (subscriber_key, ticker) values (%s, %s) "
            "on conflict do nothing", sorted(subs))
    return {"subscribers": n, "subscriptions": len(subs)}


def notify(dry_run: bool = True) -> list[dict]:
    """Resolve today's alert targets to contacts. Runs as pii_admin: the only role that can do the join."""
    with psycopg.connect(settings().dsn_for("pii_admin"), autocommit=True) as conn:
        targets = conn.execute(
            "select subscriber_key, ticker, round(vol_20d_zscore::numeric, 2) from marts.fct_alert_targets").fetchall()
        people = conn.execute("select subscriber_id, email from pii.subscribers").fetchall()
    by_key = {subscriber_key(sid): email for sid, email in people}
    out = [{"to": mask_email(by_key[k]), "ticker": t, "z": float(z)} for k, t, z in targets if k in by_key]
    for o in out:
        print(f"{'[dry run] ' if dry_run else ''}would alert {o['to']}: {o['ticker']} 20d vol z-score {o['z']}")
    return out


if __name__ == "__main__":
    print(seed() if sys.argv[1:] == ["seed"] else f"{len(notify())} notifications")
