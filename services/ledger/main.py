import os
import psycopg2
import redis as redis_lib
from confluent_kafka import Consumer, KafkaError

from events_pb2 import ExchangeEvent
from orders_pb2 import SIDE_BUY

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]

DB_DSN        = os.getenv("DATABASE_URL", "host=localhost dbname=exchange user=exchange password=exchange")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")


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
            "INSERT INTO trades (seq, symbol, aggressor_id, passive_id, price, quantity) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING seq",
            (event.seq, symbol, fill.aggressor_id, fill.passive_id, fill.price, fill.quantity),
        )
        if cur.rowcount == 0:
            return
    conn.commit()

    qty   = fill.quantity
    price = fill.price

    # look up account metadata for both sides
    aggressor_meta = r.hgetall(f"order_meta:{symbol}:{fill.aggressor_id}")
    passive_meta   = r.hgetall(f"order_meta:{symbol}:{fill.passive_id}")

    # settle aggressor side
    if aggressor_meta:
        acc = aggressor_meta[b"account_id"].decode()
        if fill.aggressor_side == SIDE_BUY:
            # buyer: reserved cash consumed, gains shares
            r.incrbyfloat(f"reserved:{acc}:cash",              -(price * qty))
            r.incrbyfloat(f"balance:{acc}:position:{symbol}",   qty)
        else:
            # seller: reserved shares consumed, gains cash
            r.incrbyfloat(f"reserved:{acc}:position:{symbol}",  -qty)
            r.incrbyfloat(f"balance:{acc}:cash",                 price * qty)

    # settle passive side (opposite direction)
    if passive_meta:
        acc = passive_meta[b"account_id"].decode()
        if fill.aggressor_side == SIDE_BUY:
            # passive is seller: reserved shares consumed, gains cash
            r.incrbyfloat(f"reserved:{acc}:position:{symbol}",  -qty)
            r.incrbyfloat(f"balance:{acc}:cash",                 price * qty)
        else:
            # passive is buyer: reserved cash consumed, gains shares
            r.incrbyfloat(f"reserved:{acc}:cash",              -(price * qty))
            r.incrbyfloat(f"balance:{acc}:position:{symbol}",   qty)


def release_cancel(r, symbol, order_id):
    meta = r.hgetall(f"order_meta:{symbol}:{order_id}")
    if not meta:
        return

    acc   = meta[b"account_id"].decode()
    price = int(meta[b"price"].decode())
    qty   = int(meta[b"qty"].decode())
    side  = meta[b"side"].decode()

    if side == "buy":
        # release remaining reserved cash back to available
        r.incrbyfloat(f"reserved:{acc}:cash", -(price * qty))
        r.incrbyfloat(f"balance:{acc}:cash",    price * qty)
    else:
        # release remaining reserved shares back to available
        r.incrbyfloat(f"reserved:{acc}:position:{symbol}", -qty)
        r.incrbyfloat(f"balance:{acc}:position:{symbol}",   qty)

    r.delete(f"order_meta:{symbol}:{order_id}")


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

    print("ledger running")

    try:
        while True:
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
            elif event.HasField("cancel"):
                release_cancel(r, event.symbol, event.cancel.order_id)

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
