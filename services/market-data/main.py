import os
import json
import time
import threading
import redis
import grpc
from concurrent import futures
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter

from metrics_util import LAG, consumer_lag, serve_metrics

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
#   md:{symbol}:trades     -> list  (capped at 100) of JSON { symbol, price, qty, side, ts_ns, taker, maker, taker_id, maker_id }
#   md:tape                -> list  (capped at 200) same JSON, all symbols merged
#   md:stats               -> hash  { trades, volume }
#   md:trader:{name}       -> hash  { trades, volume, last_ts_ms }


ACCOUNTS = {
    1: "Alice", 2: "Bob", 3: "Market Maker", 4: "Random Trader", 5: "Visitor",
    6: "Go Momentum", 7: "Rust Mean-Reversion", 8: "Java TWAP", 9: "Demo Liquidity",
}
# 100k client order id block each trader owns (the engine only has 1,000,000 id slots);
# used when order_meta is already gone (cancelled order)
ID_BLOCK = 100_000
ID_RANGES = [(100_000, 3), (200_000, 4), (300_000, 6), (400_000, 7), (500_000, 8), (600_000, 5), (800_000, 9)]


def trader_name(r: redis.Redis, sym: str, order_id: int) -> str | None:
    acc = r.hget(f"order_meta:{sym}:{order_id}", "account_id")
    if acc:
        return ACCOUNTS.get(int(acc))
    for base, account_id in ID_RANGES:
        if base <= order_id < base + ID_BLOCK:
            return ACCOUNTS[account_id]
    return None


# append the trade to the per-symbol tape and the global tape, and bump the day counters
def handle_trade_fill(r: redis.Redis, event: ExchangeEvent) -> None:
    fill = event.trade_fill
    sym  = event.symbol
    side = "buy" if fill.aggressor_side == SIDE_BUY else "sell"
    taker = trader_name(r, sym, fill.aggressor_id)
    maker = trader_name(r, sym, fill.passive_id)

    trade = json.dumps({
        "symbol":   sym,
        "price":    fill.price,
        "qty":      fill.quantity,
        "side":     side,
        "ts_ns":    event.ts_ns,
        "ts_ms":    int(time.time() * 1000),  # wall clock; ts_ns is engine-local so the UI can't age trades with it
        "taker":    taker,
        "maker":    maker,
        "taker_id": fill.aggressor_id,
        "maker_id": fill.passive_id,
    })

    pipe = r.pipeline()
    pipe.lpush(f"md:{sym}:trades", trade)
    pipe.ltrim(f"md:{sym}:trades", 0, 99)
    pipe.lpush("md:tape", trade)
    pipe.ltrim("md:tape", 0, 199)
    pipe.hincrby("md:stats", "trades", 1)
    pipe.hincrby("md:stats", "volume", fill.quantity)
    for name in (taker, maker):
        if name:
            pipe.hincrby(f"md:trader:{name}", "trades", 1)
            pipe.hincrby(f"md:trader:{name}", "volume", fill.quantity)
            pipe.hset(f"md:trader:{name}", "last_ts_ms", int(time.time() * 1000))
    pipe.execute()


def book_levels(r: redis.Redis, sym: str, side: str, n: int) -> list[tuple[int, int]]:
    """(price, volume) best first. The zset maps price -> volume, so Redis' own score order is volume order, not price order."""
    raw = r.zrange(f"md:{sym}:depth:{side}", 0, -1, withscores=True)
    return sorted(((int(p), int(v)) for p, v in raw), reverse=(side == "bid"))[:n]


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

    # recompute top of book outside pipeline (needs results); rewritten whole so an emptied side doesn't keep a stale price
    bids = book_levels(r, sym, "bid", 1)
    asks = book_levels(r, sym, "ask", 1)

    top = {}
    if bids:
        top["best_bid"], top["bid_qty"] = bids[0]
    if asks:
        top["best_ask"], top["ask_qty"] = asks[0]

    pipe = r.pipeline()
    pipe.delete(f"md:{sym}:top")
    if top:
        pipe.hset(f"md:{sym}:top", mapping=top)
    pipe.execute()


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
            raw_bids = {str(p): v for p, v in book_levels(self.r, sym, "bid", levels)}
            raw_asks = {str(p): v for p, v in book_levels(self.r, sym, "ask", levels)}

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

EVENTS = Counter("market_data_events_total", "Events the market data service handled", ["kind"])
LAG_REPORT_SECS = 5

def run_kafka(r: redis.Redis) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id":          "market-data",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([f"events.{s}" for s in SYMBOLS])

    last_lag = 0.0
    try:
        while True:
            if time.time() - last_lag >= LAG_REPORT_SECS:
                LAG.labels("market-data").set(consumer_lag(consumer))
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
                handle_trade_fill(r, event)
                EVENTS.labels("trade").inc()
            elif event.HasField("book_update"):
                handle_book_update(r, event)
                EVENTS.labels("book_update").inc()
    finally:
        consumer.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    serve_metrics(9102)
    r = redis.from_url(REDIS_URL)
    r.set("exchange:session", os.getenv("EXCHANGE_SESSION", "dev"))
    r.set("exchange:opened_at_ms", int(time.time() * 1000), nx=True)

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
