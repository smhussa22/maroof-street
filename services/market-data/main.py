import os
import json
import time
import threading
import redis
import grpc
from concurrent import futures
from confluent_kafka import Consumer, KafkaError

from events_pb2 import ExchangeEvent
from orders_pb2 import SIDE_BUY
import market_data_pb2
import market_data_pb2_grpc

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
GRPC_LISTEN   = os.getenv("GRPC_LISTEN", "0.0.0.0:50052")

POLL_INTERVAL = 0.1  # seconds between Redis polls for streaming RPCs

# Redis key schema:
#   md:{symbol}:top        -> hash  { best_bid, bid_qty, best_ask, ask_qty }
#   md:{symbol}:depth:bid  -> zset  price -> volume  (bids, highest price = best)
#   md:{symbol}:depth:ask  -> zset  price -> volume  (asks, lowest price = best)
#   md:{symbol}:trades     -> list  (capped at 100) of JSON { price, qty, side, ts_ns }


# append the trade to the trade tape for this symbol
def handle_trade_fill(r: redis.Redis, event: ExchangeEvent) -> None:
    fill = event.trade_fill
    sym  = event.symbol
    side = "buy" if fill.aggressor_side == SIDE_BUY else "sell"

    trade = json.dumps({
        "price":  fill.price,
        "qty":    fill.quantity,
        "side":   side,
        "ts_ns":  event.ts_ns,
    })

    pipe = r.pipeline()
    pipe.lpush(f"md:{sym}:trades", trade)
    pipe.ltrim(f"md:{sym}:trades", 0, 99)
    pipe.execute()


# update depth and top-of-book for a single price level change
def handle_book_update(r: redis.Redis, event: ExchangeEvent) -> None:
    update = event.book_update
    sym    = event.symbol
    is_bid = update.side == SIDE_BUY
    key    = f"md:{sym}:depth:{'bid' if is_bid else 'ask'}"

    pipe = r.pipeline()
    if update.volume == 0:
        pipe.zrem(key, str(update.price))
    else:
        pipe.zadd(key, {str(update.price): update.volume})
    pipe.execute()

    # recompute top of book outside pipeline (needs results)
    best_bid = r.zrevrange(f"md:{sym}:depth:bid", 0, 0, withscores=True)
    best_ask = r.zrange(f"md:{sym}:depth:ask", 0, 0, withscores=True)

    top = {}
    if best_bid:
        price, vol = best_bid[0]
        top["best_bid"] = price
        top["bid_qty"]  = int(vol)
    if best_ask:
        price, vol = best_ask[0]
        top["best_ask"] = price
        top["ask_qty"]  = int(vol)

    if top:
        r.hset(f"md:{sym}:top", mapping=top)


# ── gRPC servicer ────────────────────────────────────────────────────────────

class MarketDataServicer(market_data_pb2_grpc.MarketDataServicer):

    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client

    def stream_top_of_book(self, request, context):
        sym  = request.symbol
        last = None

        while context.is_active():
            raw = self.r.hgetall(f"md:{sym}:top")
            if raw and raw != last:
                last = raw
                yield market_data_pb2.TopOfBook(
                    symbol   = sym,
                    ts_ns    = time.time_ns(),
                    best_bid = int(raw.get(b"best_bid", 0)),
                    bid_qty  = int(raw.get(b"bid_qty",  0)),
                    best_ask = int(raw.get(b"best_ask", 0)),
                    ask_qty  = int(raw.get(b"ask_qty",  0)),
                )
            time.sleep(POLL_INTERVAL)

    def stream_depth(self, request, context):
        sym    = request.symbol
        levels = request.levels or 10
        prev_bids: dict[str, float] = {}
        prev_asks: dict[str, float] = {}
        snapshot_sent = False

        while context.is_active():
            raw_bids = {
                p.decode(): s for p, s in
                self.r.zrevrange(f"md:{sym}:depth:bid", 0, levels - 1, withscores=True)
            }
            raw_asks = {
                p.decode(): s for p, s in
                self.r.zrange(f"md:{sym}:depth:ask", 0, levels - 1, withscores=True)
            }

            if not snapshot_sent:
                yield market_data_pb2.DepthUpdate(
                    symbol      = sym,
                    ts_ns       = time.time_ns(),
                    is_snapshot = True,
                    bids        = [market_data_pb2.PriceLevel(price=int(p), volume=int(v)) for p, v in raw_bids.items()],
                    asks        = [market_data_pb2.PriceLevel(price=int(p), volume=int(v)) for p, v in raw_asks.items()],
                )
                prev_bids = raw_bids
                prev_asks = raw_asks
                snapshot_sent = True
            else:
                changed_bids = [
                    market_data_pb2.PriceLevel(price=int(p), volume=int(raw_bids.get(p, 0)))
                    for p in set(prev_bids) | set(raw_bids)
                    if prev_bids.get(p) != raw_bids.get(p)
                ]
                changed_asks = [
                    market_data_pb2.PriceLevel(price=int(p), volume=int(raw_asks.get(p, 0)))
                    for p in set(prev_asks) | set(raw_asks)
                    if prev_asks.get(p) != raw_asks.get(p)
                ]

                if changed_bids or changed_asks:
                    yield market_data_pb2.DepthUpdate(
                        symbol      = sym,
                        ts_ns       = time.time_ns(),
                        is_snapshot = False,
                        bids        = changed_bids,
                        asks        = changed_asks,
                    )
                    prev_bids = raw_bids
                    prev_asks = raw_asks

            time.sleep(POLL_INTERVAL)

    def stream_trades(self, request, context):
        sym      = request.symbol
        last_ts  = None

        while context.is_active():
            raw = self.r.lrange(f"md:{sym}:trades", 0, 49)

            new_trades = []
            for entry in raw:
                t = json.loads(entry)
                if last_ts is None or t["ts_ns"] > last_ts:
                    new_trades.append(t)

            # emit oldest-first
            for t in reversed(new_trades):
                side = market_data_pb2.Trade.SIDE_BUY if t["side"] == "buy" else market_data_pb2.Trade.SIDE_SELL
                yield market_data_pb2.Trade(
                    symbol        = sym,
                    ts_ns         = t["ts_ns"],
                    price         = t["price"],
                    quantity      = t["qty"],
                    aggressor_side = side,
                )
                last_ts = t["ts_ns"]

            time.sleep(POLL_INTERVAL)


# ── Kafka consumer thread ─────────────────────────────────────────────────────

def run_kafka(r: redis.Redis) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id":          "market-data",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([f"events.{s}" for s in SYMBOLS])

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
    finally:
        consumer.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    r = redis.from_url(REDIS_URL)

    kafka_thread = threading.Thread(target=run_kafka, args=(r,), daemon=True)
    kafka_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market_data_pb2_grpc.add_MarketDataServicer_to_server(MarketDataServicer(r), server)
    server.add_insecure_port(GRPC_LISTEN)
    server.start()

    print(f"market-data running — gRPC on {GRPC_LISTEN}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
