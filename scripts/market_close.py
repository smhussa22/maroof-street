"""
Closing bell. Run by `./start stop` after order intake has been stopped (bots, gateway-http, gateway are down).

1. wait until the ledger and market-data have consumed every event the engines published
2. read the session totals from Redis and print the day summary
3. record the session in Postgres

Kafka and Redis have no volumes, so the `docker-compose down` that follows leaves the next start with an empty book.
"""
import json
import os
import subprocess
import time

import psycopg2
import redis

DB_DSN    = os.getenv("DATABASE_URL", "host=localhost dbname=exchange user=exchange password=exchange")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
COMPOSE   = ["docker-compose", "-f", "deploy/docker-compose.yml"]
DRAIN_TIMEOUT_SECS = 30


def group_lag(group: str) -> int:
    out = subprocess.run(
        COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                   "--bootstrap-server", "localhost:9092", "--describe", "--group", group],
        capture_output=True, text=True,
    ).stdout
    # columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
    return sum(int(p[5]) for p in (line.split() for line in out.splitlines())
               if len(p) >= 6 and p[0] == group and p[5].isdigit())


def wait_for_drain() -> bool:
    deadline = time.time() + DRAIN_TIMEOUT_SECS
    while time.time() < deadline:
        if group_lag("ledger") == 0 and group_lag("market-data") == 0:
            return True
        time.sleep(1)
    return False


def main() -> None:
    drained = wait_for_drain()

    r = redis.from_url(REDIS_URL)
    session   = (r.get("exchange:session") or b"unknown").decode()
    opened_ms = int(r.get("exchange:opened_at_ms") or 0) or None
    stats     = r.hgetall("md:stats")
    trades    = int(stats.get(b"trades", 0))
    volume    = int(stats.get(b"volume", 0))

    traders = {}
    for key in r.scan_iter("md:trader:*"):
        h = r.hgetall(key)
        traders[key.decode().split(":", 2)[2]] = {
            "trades": int(h.get(b"trades", 0)),
            "volume": int(h.get(b"volume", 0)),
        }

    conn = psycopg2.connect(DB_DSN)
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sessions (session, opened_at, trades, volume, traders) "
            "VALUES (%s, to_timestamp(%s / 1000.0), %s, %s, %s) "
            "ON CONFLICT (session) DO UPDATE SET closed_at = NOW(), trades = EXCLUDED.trades, "
            "volume = EXCLUDED.volume, traders = EXCLUDED.traders",
            (session, opened_ms, trades, volume, json.dumps(traders)),
        )
    conn.close()

    minutes = (time.time() * 1000 - opened_ms) / 60000 if opened_ms else 0
    print(f"\nThe market is closed. Session {session}, open for {minutes:.1f} minutes.")
    print(f"  {trades} trades, {volume} shares traded")
    for name, t in sorted(traders.items(), key=lambda kv: -kv[1]["trades"]):
        print(f"  {name:<20} {t['trades']:>6} trades {t['volume']:>8} shares")
    if not drained:
        print(f"  warning: consumers were still catching up after {DRAIN_TIMEOUT_SECS}s, so the totals may be slightly low")


if __name__ == "__main__":
    main()
