import hmac
import os
import sys
import json
import time
from typing import Literal

import grpc
import redis as redis_lib
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ledger'))

from orders_pb2 import SubmitOrder, ORDER_TYPE_GTC, ORDER_TYPE_FAK, SIDE_BUY, SIDE_SELL
import orders_pb2_grpc
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
TRADERS = ["Market Maker", "Random Trader", "Go Momentum", "Rust Mean-Reversion", "Java TWAP", "Visitor"]

app = FastAPI()
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


@app.post("/order")
def submit_order(body: OrderRequest, x_client_ip: str = Header("unknown"), x_proxy_secret: str = Header("")):
    # X-Client-IP drives the rate limit, so only the website's server (which knows the secret) may set it
    if PROXY_SECRET and not hmac.compare_digest(x_proxy_secret, PROXY_SECRET):
        return JSONResponse({"accepted": False, "reason": "forbidden"}, status_code=403)

    lo, hi = price_band(reference_price(body.symbol))
    if not lo <= body.price < hi:
        return {"accepted": False, "reason": "price_out_of_band", "lo": lo, "hi": hi}

    cooldown_key = f"rl:order:{x_client_ip}"
    if not r.set(cooldown_key, 1, nx=True, ex=COOLDOWN_SECS):
        return {"accepted": False, "reason": "rate_limited", "retry_after": max(r.ttl(cooldown_key), 1)}
    hour_key = f"rl:hour:{x_client_ip}"
    count = r.incr(hour_key)
    if count == 1:
        r.expire(hour_key, 3600)
    if count > HOURLY_LIMIT:
        return {"accepted": False, "reason": "hourly_limit", "retry_after": max(r.ttl(hour_key), 1)}

    # shortcut: a Redis-only counter, so a Redis restart without a Kafka restart could reuse ids; both are wiped together on teardown
    order_id = ORDER_ID_BASE + r.incr("demo:order_id") % ORDER_ID_BLOCK
    req = SubmitOrder(
        api_key         = DEMO_API_KEY,
        symbol          = body.symbol,
        price           = body.price,
        quantity        = body.quantity,
        order_direction = SIDE_BUY if body.side == "buy" else SIDE_SELL,
        order_type      = ORDER_TYPE_FAK if body.type == "fak" else ORDER_TYPE_GTC,
        client_order_id = order_id,
    )

    try:
        ack = stub.submit_order(req, metadata=[("x-api-key", DEMO_API_KEY)], timeout=5)
        return {"accepted": ack.accepted, "reason": ack.rejected_reason, "order_id": order_id}
    except grpc.RpcError as e:
        unavailable = e.code() == grpc.StatusCode.UNAVAILABLE
        return {"accepted": False, "reason": "gateway_unavailable" if unavailable else e.details()}


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
def account():
    def num(key):
        return int(round(float(r.get(key) or 0)))
    return {
        "cash":      num(f"balance:{DEMO_ACCOUNT}:cash"),
        "reserved":  num(f"reserved:{DEMO_ACCOUNT}:cash"),
        "positions": {s: num(f"balance:{DEMO_ACCOUNT}:position:{s}") for s in SYMBOLS},
    }


@app.get("/order/{symbol}/{order_id}")
def order_status(symbol: str, order_id: int):
    check_symbol(symbol)
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
