import os
import time
import psycopg2
import redis as redis_lib
from confluent_kafka import Consumer, KafkaError

from prometheus_client import Counter

from balances import expire_fak, release_remaining, settle_balances
from metrics_util import LAG, consumer_lag, serve_metrics
from events_pb2 import (
    ExchangeEvent,
    REJECTED_REASON_INVALID_PRICE,
    REJECTED_REASON_INVALID_QUANTITY,
    REJECTED_REASON_POOL_FULL,
)

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]

DB_DSN = os.getenv("DATABASE_URL", "host=localhost dbname=exchange user=exchange password=exchange")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SESSION = os.getenv("EXCHANGE_SESSION", "dev")

FAK_SWEEP_SECS = 1
LAG_REPORT_SECS = 5

EVENTS = Counter("ledger_events_total", "Events the ledger handled", ["kind"])

# reasons that mean a submitted order never reached the book; duplicate-id and not-found rejects are left out
# because their id can belong to a different live order
SUBMIT_REJECTS = {REJECTED_REASON_INVALID_PRICE, REJECTED_REASON_INVALID_QUANTITY, REJECTED_REASON_POOL_FULL}


def apply_schema(conn):
    schema = open(os.path.join(os.path.dirname(__file__), "schema.sql")).read()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()


def settle(conn, r, event, fill):
    symbol = event.symbol

    with conn.cursor() as cur:
        # idempotency: skip if already settled
        cur.execute(
            "INSERT INTO trades (session, seq, symbol, aggressor_id, passive_id, price, quantity) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING seq",
            (SESSION, event.seq, symbol, fill.aggressor_id, fill.passive_id, fill.price, fill.quantity),
        )
        if cur.rowcount == 0:
            return
    conn.commit()

    settle_balances(r, symbol, fill)


def main():
    conn = psycopg2.connect(DB_DSN)
    apply_schema(conn)

    r = redis_lib.from_url(REDIS_URL)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "ledger",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([f"events.{s}" for s in SYMBOLS])

    serve_metrics(9101)
    print("ledger running", flush=True)

    last_sweep = 0.0
    last_lag = 0.0
    try:
        while True:
            # release the unfilled remainder of FAK orders whose grace period has passed
            if time.time() - last_sweep >= FAK_SWEEP_SECS:
                expire_fak(r)
                last_sweep = time.time()
            if time.time() - last_lag >= LAG_REPORT_SECS:
                LAG.labels("ledger").set(consumer_lag(consumer))
                last_lag = time.time()

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"kafka error: {msg.error()}")
                continue

            event = ExchangeEvent()
            event.ParseFromString(msg.value())

            if event.HasField("trade_fill"):
                settle(conn, r, event, event.trade_fill)
                EVENTS.labels("trade").inc()
            elif event.HasField("cancel"):
                release_remaining(r, event.symbol, event.cancel.order_id)
                EVENTS.labels("cancel").inc()
            elif event.HasField("reject") and event.reject.reason in SUBMIT_REJECTS:
                release_remaining(r, event.symbol, event.reject.order_id)
                EVENTS.labels("reject").inc()

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
