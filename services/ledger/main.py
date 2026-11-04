import os
import psycopg2
from confluent_kafka import Consumer, KafkaError

from events_pb2 import ExchangeEvent

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]

DB_DSN = os.getenv("DATABASE_URL", "host=localhost dbname=maroof user=postgres password=postgres")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

# 1. connects to postgres, runs schema.sql to make tabels
# 2. create a kafka consumer subscribed to events.<SYMBOL>
# 3. deserialize the raw bytes back into an ExchangeEvent proto object
# 4. if the event is a trade_fill, settle it in Postgres; debit buyer cash/credit position, credit seller cash/debit position, all in one transaction

    # debit -> subtract from an account (cash goes down, or shares go down)
    # credit -> add to an account (cash goes up, or shares go up)
    # position -> how many shares of a symbol an account holds

    # buyer paid cash to get shares -> cash goes down (debit), position goes up (credit)
    # seller gave up shares to get cash -> cash goes up (credit), position goes down (debit)

# 5. if the event is anything else (ack, cancel, book update), ignore it and poll again; those dont need to persist

def apply_schema(conn):
    schema = open(os.path.join(os.path.dirname(__file__), "schema.sql")).read()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()


def settle(conn, event, fill):
    with conn.cursor() as cur:
        # idempotency: skip if already settled
        cur.execute(
            "INSERT INTO trades (seq, symbol, aggressor_id, passive_id, price, quantity) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING seq",
            (event.seq, event.symbol, fill.aggressor_id, fill.passive_id, fill.price, fill.quantity),
        )
        if cur.rowcount == 0:
            return  # already settled

        # TODO: balance/position settlement requires account mapping (order_id -> account_id)
        # will be wired up in Stage 5 when API key -> account is in place

    conn.commit()


def main():
    conn = psycopg2.connect(DB_DSN)
    apply_schema(conn)

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
                settle(conn, event, event.trade_fill)

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
