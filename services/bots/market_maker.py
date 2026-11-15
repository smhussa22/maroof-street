"""
Market maker bot — maintains LEVELS resting bids and asks on every symbol.
Cancels and re-posts every REFRESH_SECS to track the current mid price.
"""
import os
import sys
import time

import grpc
import redis as redis_lib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ledger'))

from orders_pb2 import SubmitOrder, CancelOrder, SIDE_BUY, SIDE_SELL, ORDER_TYPE_GTC
import orders_pb2_grpc

GATEWAY_ADDR  = os.getenv("GATEWAY_ADDR", "localhost:50051")
REDIS_URL     = os.getenv("REDIS_URL",    "redis://localhost:6379")
MM_API_KEY    = os.getenv("MM_API_KEY",   "mm-bot-key-111")

SYMBOLS       = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]
REF_PRICES    = {"AAPL": 150, "TSLA": 250, "NVDA": 400, "MSFT": 300, "GOOGL": 170}
SPREAD        = 2    # ticks each side of mid
QTY           = 50   # shares at the inner level; level n quotes QTY * (n + 1)
LEVELS        = 2    # quotes per side; about 8 requests/s stays under the gateway's 10/s per-account limit
REFRESH_SECS  = 5

# the engine indexes live orders by id and only has 1,000,000 slots, so each trader owns a 100k block that wraps
ORDER_ID_BASE = 100_000
ORDER_ID_BLOCK = 100_000


def mid_price(r: redis_lib.Redis, sym: str) -> int:
    top = r.hgetall(f"md:{sym}:top")
    if top:
        bid = int(top.get(b"best_bid", 0) or 0)
        ask = int(top.get(b"best_ask", 0) or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) // 2
        if bid > 0:
            return bid + SPREAD
        if ask > 0:
            return ask - SPREAD
    return REF_PRICES[sym]


def main() -> None:
    r       = redis_lib.from_url(REDIS_URL)
    channel = grpc.insecure_channel(GATEWAY_ADDR)
    stub    = orders_pb2_grpc.OrderServiceStub(channel)

    active  = {sym: [] for sym in SYMBOLS}  # client order ids currently resting, per symbol
    counter = 0
    mm_meta = [("x-api-key", MM_API_KEY)]

    print("market-maker running", flush=True)

    while True:
        for sym in SYMBOLS:
            for oid in active[sym]:
                try:
                    stub.cancel_order(
                        CancelOrder(api_key=MM_API_KEY, symbol=sym, order_id=oid),
                        metadata=mm_meta,
                        timeout=3,
                    )
                except Exception:
                    pass
            active[sym] = []

            mid = mid_price(r, sym)
            try:
                for level in range(LEVELS):
                    for direction, price in ((SIDE_BUY, mid - SPREAD - level), (SIDE_SELL, mid + SPREAD + level)):
                        oid = ORDER_ID_BASE + counter % ORDER_ID_BLOCK
                        counter += 1
                        stub.submit_order(SubmitOrder(
                            api_key=MM_API_KEY, symbol=sym,
                            price=price, quantity=QTY * (level + 1),
                            order_direction=direction, order_type=ORDER_TYPE_GTC,
                            client_order_id=oid,
                        ), metadata=mm_meta, timeout=3)
                        active[sym].append(oid)
            except Exception as e:
                print(f"mm {sym}: {e}", flush=True)

        time.sleep(REFRESH_SECS)


if __name__ == "__main__":
    main()
