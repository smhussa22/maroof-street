import os
import json
import redis
from confluent_kafka import Consumer, KafkaError

from events_pb2 import ExchangeEvent
from orders_pb2 import OrderDirection

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")

# Redis key schema:
#   md:{symbol}:top        -> hash  { best_bid, bid_qty, best_ask, ask_qty }
#   md:{symbol}:depth:bid  -> zset  price -> volume  (bids, highest price = best)
#   md:{symbol}:depth:ask  -> zset  price -> volume  (asks, lowest price = best)
#   md:{symbol}:trades     -> list  (capped at 100) of JSON { price, qty, side, ts_ns }


# append the trade to the trade tape for this symbol
# serialize fill fields to JSON, lpush onto md:{sym}:trades, ltrim to 100 entries
def handle_trade_fill(r: redis.Redis, event: ExchangeEvent) -> None:
    fill = event.trade_fill
    sym  = event.symbol

    side = "buy" if fill.aggressor_side == OrderDirection.Value("SIDE_BUY") else "sell"

    trade = json.dumps({
        "price":  fill.price,
        "qty":    fill.quantity,
        "side":   side,
        "ts_ns":  event.ts_ns,
    })

    pipe = r.pipeline()
    pipe.lpush(f"md:{sym}:trades", trade)
    pipe.ltrim(f"md:{sym}:trades", 0, 99)   # keep last 100 trades
    pipe.execute()


# update the depth and top-of-book in Redis for a single price level change
# add or remove the level from the bid/ask zset, then recompute best bid/ask into md:{sym}:top
def handle_book_update(r: redis.Redis, event: ExchangeEvent) -> None:
    update = event.book_update
    sym    = event.symbol

    is_bid = update.side == OrderDirection.Value("SIDE_BUY")
    key    = f"md:{sym}:depth:{'bid' if is_bid else 'ask'}"

    pipe = r.pipeline()

    if update.volume == 0:
        # level cleared
        pipe.zrem(key, update.price)
    else:
        pipe.zadd(key, {str(update.price): update.volume})

    # recompute top of book
    best_bid_members = r.zrevrange("md:{sym}:depth:bid", 0, 0, withscores=True)
    best_ask_members = r.zrange( "md:{sym}:depth:ask", 0, 0, withscores=True)

    top = {}
    if best_bid_members:
        price, vol = best_bid_members[0]
        top["best_bid"] = price
        top["bid_qty"]  = int(vol)
    if best_ask_members:
        price, vol = best_ask_members[0]
        top["best_ask"] = price
        top["ask_qty"]  = int(vol)

    if top:
        pipe.hset(f"md:{sym}:top", mapping=top)

    pipe.execute()


# consume events.<SYMBOL> from Kafka and dispatch trade fills and book updates to Redis
# connect to Redis and Kafka, subscribe to all symbols, poll forever and route by event type
def main() -> None:
    r = redis.from_url(REDIS_URL)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id":          "market-data",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([f"events.{s}" for s in SYMBOLS])

    print("market-data running")

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
                handle_trade_fill(r, event)
            elif event.HasField("book_update"):
                handle_book_update(r, event)
            # ack, cancel, reject don't affect market data

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
