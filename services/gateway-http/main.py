import hmac
import os
import sys
import json
import re
import threading
import time
from typing import Literal

import grpc
import redis as redis_lib
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import BaseModel, Field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ledger'))

from orders_pb2 import SubmitOrder, ORDER_TYPE_GTC, ORDER_TYPE_FAK, SIDE_BUY, SIDE_SELL
import orders_pb2_grpc
import wallet
from market import REF_PRICES, price_band, summarize

GATEWAY_ADDR = os.getenv("GATEWAY_ADDR", "localhost:50051")
REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379")
DEMO_API_KEY = os.getenv("DEMO_API_KEY", "demo-key-789")
PROXY_SECRET = os.getenv("PROXY_SECRET", "")  # when set, POST /order only accepts requests that carry it

SYMBOLS       = list(REF_PRICES)
DEMO_ACCOUNT  = 5
ORDER_ID_BASE  = 600_000  # market-data's ID_RANGES maps this 100k block to "Visitor"
ORDER_ID_BLOCK = 100_000  # the engine only has 1,000,000 id slots, so ids wrap inside the block
COOLDOWN_SECS = 3
HOURLY_LIMIT  = 30
HOUSE_API_KEY = os.getenv("HOUSE_API_KEY", "house-key-999")
HOUSE_ID_BASE = 800_000  # market-data's ID_RANGES maps this block to "Demo Liquidity"
HOUSE_WAIT_SECS = 0.4    # how long a visitor order gets to fill against the real book before the house steps in
SETTLE_TRIES = 24
VISITOR_ID = re.compile(r"^[0-9a-f-]{36}$")

ORDERS = Counter("exchange_orders_total", "Visitor orders by outcome", ["result"])
SUBMIT_SECONDS = Histogram("exchange_submit_seconds", "Time for the order gateway to answer a submitted order",
                           buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1))
HOUSE_FILLS = Counter("exchange_house_orders_total", "Orders the demo liquidity account placed to complete a visitor order")

TRADERS = ["Market Maker", "Random Trader", "Go Momentum", "Rust Mean-Reversion", "Java TWAP", "Visitor"]

app = FastAPI()
app.mount("/metrics", make_asgi_app())
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r       = redis_lib.from_url(REDIS_URL)
channel = grpc.insecure_channel(GATEWAY_ADDR)
stub    = orders_pb2_grpc.OrderServiceStub(channel)


@app.exception_handler(RequestValidationError)
def invalid_order(_, exc):
    return JSONResponse({"accepted": False, "reason": "invalid_order"}, status_code=422)


class OrderRequest(BaseModel):
    symbol:   Literal["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]
    side:     Literal["buy", "sell"]
    price:    int = Field(gt=0, lt=1_000_000)
    quantity: int = Field(ge=1, le=100)
    type:     Literal["gtc", "fak"] = "gtc"


def check_symbol(symbol: str) -> None:
    if symbol not in SYMBOLS:
        raise HTTPException(404, "unknown symbol")


def last_trades(symbol: str, n: int = 100) -> list[dict]:
    return [json.loads(t) for t in r.lrange(f"md:{symbol}:trades", 0, n - 1)]


def book_levels(symbol: str, side: str, n: int) -> list[dict]:
    # the depth zset maps price -> volume, so Redis' score order is volume order; sort by price here
    raw = r.zrange(f"md:{symbol}:depth:{side}", 0, -1, withscores=True)
    levels = sorted(((int(p), int(v)) for p, v in raw), reverse=(side == "bid"))
    return [{"price": p, "volume": v} for p, v in levels[:n]]


def reference_price(symbol: str) -> int:
    top = r.hgetall(f"md:{symbol}:top")
    bid = float(top.get(b"best_bid") or 0)
    ask = float(top.get(b"best_ask") or 0)
    if bid and ask:
        return int((bid + ask) / 2)
    if bid or ask:
        return int(bid or ask)
    latest = r.lindex(f"md:{symbol}:trades", 0)
    return json.loads(latest)["price"] if latest else REF_PRICES[symbol]


def visitor_of(header: str, ip: str) -> str:
    # the website sends an anonymous id per browser; anything else falls back to one wallet per address
    return header if VISITOR_ID.match(header) else f"ip-{ip}"


def fills_for(symbol: str, order_id: int) -> list[dict]:
    """Every fill of one order from the recent tape, oldest first."""
    out = []
    for t in reversed(last_trades(symbol)):
        if order_id not in (t.get("taker_id"), t.get("maker_id")):
            continue
        mine_is_taker = t.get("taker_id") == order_id
        other_id = t.get("maker_id") if mine_is_taker else t.get("taker_id")
        out.append({
            "price": t["price"], "qty": t["qty"], "ts_ms": t.get("ts_ms"),
            "counterparty": t.get("maker") if mine_is_taker else t.get("taker"),
            "house": other_id is not None and HOUSE_ID_BASE <= other_id < HOUSE_ID_BASE + ORDER_ID_BLOCK,
        })
    return out


def house_order(symbol: str, side: str, price: int, qty: int) -> None:
    """Takes the other side of a visitor order at the visitor's price, so every demo order completes the full path."""
    house_id = HOUSE_ID_BASE + r.incr("demo:house_id") % ORDER_ID_BLOCK
    req = SubmitOrder(
        api_key         = HOUSE_API_KEY,
        symbol          = symbol,
        price           = price,
        quantity        = qty,
        order_direction = SIDE_SELL if side == "buy" else SIDE_BUY,
        order_type      = ORDER_TYPE_GTC,
        client_order_id = house_id,
    )
    stub.submit_order(req, metadata=[("x-api-key", HOUSE_API_KEY)], timeout=5)
    HOUSE_FILLS.inc()


def finish_order(visitor: str, symbol: str, order_id: int, side: str, price: int, qty: int) -> None:
    """Background: let the real book fill the order, cover whatever is left with the house, then settle the wallet."""
    try:
        time.sleep(HOUSE_WAIT_SECS)
        if sum(f["qty"] for f in fills_for(symbol, order_id)) < qty:
            remaining = qty - sum(f["qty"] for f in fills_for(symbol, order_id))
            if remaining > 0:
                house_order(symbol, side, price, remaining)
        for _ in range(SETTLE_TRIES):
            fills = fills_for(symbol, order_id)
            wallet.settle(r, visitor, order_id, fills, now_ms=int(time.time() * 1000))
            if sum(f["qty"] for f in fills) >= qty:
                return
            time.sleep(0.25)
    except Exception as e:  # a failed background step must never take the service down
        print(f"finish_order {symbol} {order_id} failed: {e}", flush=True)


@app.post("/order")
def submit_order(
    body: OrderRequest,
    x_client_ip: str = Header("unknown"),
    x_proxy_secret: str = Header(""),
    x_visitor_id: str = Header(""),
):
    # X-Client-IP drives the rate limit, so only the website's server (which knows the secret) may set it
    if PROXY_SECRET and not hmac.compare_digest(x_proxy_secret, PROXY_SECRET):
        return JSONResponse({"accepted": False, "reason": "forbidden"}, status_code=403)

    received_ms = int(time.time() * 1000)

    def refuse(reason: str, **extra):
        ORDERS.labels(reason).inc()
        return {"accepted": False, "reason": reason, **extra}

    lo, hi = price_band(reference_price(body.symbol))
    if not lo <= body.price < hi:
        return refuse("price_out_of_band", lo=lo, hi=hi)

    cooldown_key = f"rl:order:{x_client_ip}"
    if not r.set(cooldown_key, 1, nx=True, ex=COOLDOWN_SECS):
        return refuse("rate_limited", retry_after=max(r.ttl(cooldown_key), 1))
    hour_key = f"rl:hour:{x_client_ip}"
    count = r.incr(hour_key)
    if count == 1:
        r.expire(hour_key, 3600)
    if count > HOURLY_LIMIT:
        return refuse("hourly_limit", retry_after=max(r.ttl(hour_key), 1))

    visitor = visitor_of(x_visitor_id, x_client_ip)
    unaffordable = wallet.check_funds(r, visitor, body.side, body.symbol, body.quantity, body.price)
    if unaffordable:
        return refuse(unaffordable)

    # shortcut: a Redis-only counter, so a Redis restart without a Kafka restart could reuse ids; both are wiped together on teardown
    order_id = ORDER_ID_BASE + r.incr("demo:order_id") % ORDER_ID_BLOCK
    # every visitor order rests in the book until it fills (the form's order type is ignored), so the house can complete it
    req = SubmitOrder(
        api_key         = DEMO_API_KEY,
        symbol          = body.symbol,
        price           = body.price,
        quantity        = body.quantity,
        order_direction = SIDE_BUY if body.side == "buy" else SIDE_SELL,
        order_type      = ORDER_TYPE_GTC,
        client_order_id = order_id,
    )

    try:
        started = time.perf_counter()
        ack = stub.submit_order(req, metadata=[("x-api-key", DEMO_API_KEY)], timeout=5)
        SUBMIT_SECONDS.observe(time.perf_counter() - started)
    except grpc.RpcError as e:
        unavailable = e.code() == grpc.StatusCode.UNAVAILABLE
        return refuse("gateway_unavailable" if unavailable else e.details())

    if not ack.accepted:
        return refuse(ack.rejected_reason or "rejected")

    ORDERS.labels("accepted").inc()
    wallet.open_order(r, visitor, order_id, {
        "symbol": body.symbol, "side": body.side, "qty": body.quantity, "price": body.price,
        "sent_ms": received_ms, "received_ms": received_ms, "acked_ms": int(time.time() * 1000),
    })
    threading.Thread(
        target=finish_order, args=(visitor, body.symbol, order_id, body.side, body.price, body.quantity), daemon=True
    ).start()
    return {"accepted": True, "reason": "", "order_id": order_id, "simulated": False}


_status_cache = (0.0, False)


@app.get("/status")
def status():
    global _status_cache
    now = time.time()
    if now - _status_cache[0] > 2:
        try:
            r.ping()
            grpc.channel_ready_future(channel).result(timeout=0.5)
            up = True
        except Exception:
            up = False
        _status_cache = (now, up)
    stats = r.hgetall("md:stats")
    return {
        "open":      _status_cache[1],
        "trades":    int(stats.get(b"trades", 0)),
        "volume":    int(stats.get(b"volume", 0)),
        "server_ms": int(now * 1000),
    }


@app.get("/market")
def market():
    now_ms = int(time.time() * 1000)
    symbols = []
    for sym in SYMBOLS:
        top  = r.hgetall(f"md:{sym}:top")
        resting = sum(
            int(score)
            for side in ("bid", "ask")
            for _, score in r.zrange(f"md:{sym}:depth:{side}", 0, -1, withscores=True)
        )
        symbols.append({
            "symbol":   sym,
            "best_bid": int(float(top.get(b"best_bid") or 0)),
            "best_ask": int(float(top.get(b"best_ask") or 0)),
            "resting":  resting,
            **summarize(last_trades(sym)),
        })

    tape = [json.loads(t) for t in r.lrange("md:tape", 0, 199)]
    stats = r.hgetall("md:stats")
    return {
        "server_ms": now_ms,
        "symbols":   symbols,
        "stats": {
            "trades_total":   int(stats.get(b"trades", 0)),
            "volume_total":   int(stats.get(b"volume", 0)),
            "trades_per_min": sum(1 for t in tape if t.get("ts_ms", 0) > now_ms - 60_000),
            "last_trade_ms":  tape[0].get("ts_ms") if tape else None,
            "resting":        sum(s["resting"] for s in symbols),
        },
    }


@app.get("/tape")
def tape(limit: int = Query(30, ge=1, le=100)):
    return [json.loads(t) for t in r.lrange("md:tape", 0, limit - 1)]


@app.get("/traders")
def traders():
    out = []
    for name in TRADERS:
        h = r.hgetall(f"md:trader:{name}")
        out.append({
            "name":        name,
            "trades":      int(h.get(b"trades", 0)),
            "volume":      int(h.get(b"volume", 0)),
            "last_ts_ms":  int(h[b"last_ts_ms"]) if b"last_ts_ms" in h else None,
        })
    return out


@app.get("/account")
def account(x_visitor_id: str = Header(""), x_client_ip: str = Header("unknown")):
    if VISITOR_ID.match(x_visitor_id):
        return wallet.view(r, x_visitor_id)
    def num(key):
        return int(round(float(r.get(key) or 0)))
    return {
        "cash":      num(f"balance:{DEMO_ACCOUNT}:cash"),
        "reserved":  num(f"reserved:{DEMO_ACCOUNT}:cash"),
        "positions": {s: num(f"balance:{DEMO_ACCOUNT}:position:{s}") for s in SYMBOLS},
    }


@app.get("/order/{symbol}/{order_id}")
def order_status(symbol: str, order_id: int, x_visitor_id: str = Header("")):
    check_symbol(symbol)
    mine = wallet.read_order(r, x_visitor_id, order_id) if VISITOR_ID.match(x_visitor_id) else None
    if mine:
        fills = mine["fills"]
        return {
            "known": True, "qty": mine["qty"], "price": mine["price"],
            "filled": sum(f["qty"] for f in fills), "fills": fills,
            "trace": {
                "received_ms": mine.get("received_ms"), "acked_ms": mine.get("acked_ms"),
                "first_fill_ms": min((f["ts_ms"] for f in fills if f.get("ts_ms")), default=None),
                "settled_ms": mine.get("settled_ms"),
            },
        }
    meta  = r.hgetall(f"order_meta:{symbol}:{order_id}")
    fills = [
        {"price": t["price"], "qty": t["qty"], "ts_ms": t.get("ts_ms"),
         "counterparty": t.get("maker") if t.get("taker_id") == order_id else t.get("taker")}
        for t in last_trades(symbol)
        if order_id in (t.get("taker_id"), t.get("maker_id"))
    ]
    return {
        "known":  bool(meta) or bool(fills),
        "qty":    int(meta[b"qty"]) if meta else None,
        "price":  int(meta[b"price"]) if meta else None,
        "filled": sum(f["qty"] for f in fills),
        "fills":  fills,
    }


@app.get("/book/{symbol}")
def get_book(symbol: str, levels: int = Query(10, ge=1, le=20)):
    check_symbol(symbol)
    top = r.hgetall(f"md:{symbol}:top")
    return {
        "symbol": symbol,
        "top":  {k.decode(): int(float(v)) for k, v in top.items()} if top else {},
        "bids": book_levels(symbol, "bid", levels),
        "asks": book_levels(symbol, "ask", levels),
    }


@app.get("/trades/{symbol}")
def get_trades(symbol: str):
    check_symbol(symbol)
    return last_trades(symbol, 20)
