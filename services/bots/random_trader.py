"""
Random trader bot — fires aggressive FAK orders every few seconds to generate fills.
Reads the current top of book from Redis and prices to cross the spread.
"""
import os
import sys
import time
import random

import grpc
import redis as redis_lib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ledger'))

from orders_pb2 import SubmitOrder, SIDE_BUY, SIDE_SELL, ORDER_TYPE_FAK
import orders_pb2_grpc

GATEWAY_ADDR = os.getenv("GATEWAY_ADDR", "localhost:50051")
REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379")
RT_API_KEY   = os.getenv("RT_API_KEY",   "rt-bot-key-222")

SYMBOLS    = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]
REF_PRICES = {"AAPL": 150, "TSLA": 250, "NVDA": 400, "MSFT": 300, "GOOGL": 170}

ORDER_ID_BASE = 200_000  # 100k block; the engine only has 1,000,000 id slots
ORDER_ID_BLOCK = 100_000


def main() -> None:
    r       = redis_lib.from_url(REDIS_URL)
    channel = grpc.insecure_channel(GATEWAY_ADDR)
    stub    = orders_pb2_grpc.OrderServiceStub(channel)
    counter = 0

    print("random-trader running")

    while True:
        time.sleep(random.uniform(1.5, 4))

        sym  = random.choice(SYMBOLS)
        side = random.choice([SIDE_BUY, SIDE_SELL])
        qty  = random.randint(1, 20)

        top = r.hgetall(f"md:{sym}:top")
        if side == SIDE_BUY:
            ref = int(top.get(b"best_ask", 0) or 0) if top else 0
            if ref == 0:
                ref = REF_PRICES[sym] + 2
            price = ref + random.randint(0, 2)
        else:
            ref = int(top.get(b"best_bid", 0) or 0) if top else 0
            if ref == 0:
                ref = REF_PRICES[sym] - 2
            price = ref - random.randint(0, 2)

        order_id = ORDER_ID_BASE + counter % ORDER_ID_BLOCK
        counter += 1

        try:
            ack = stub.submit_order(SubmitOrder(
                api_key         = RT_API_KEY,
                symbol          = sym,
                price           = price,
                quantity        = qty,
                order_direction = side,
                order_type      = ORDER_TYPE_FAK,
                client_order_id = order_id,
            ), metadata=[("x-api-key", RT_API_KEY)], timeout=3)
            if ack.accepted:
                side_str = "buy" if side == SIDE_BUY else "sell"
                print(f"{sym} {side_str} {qty}@{price}")
        except Exception as e:
            print(f"rt error: {e}")


if __name__ == "__main__":
    main()
