"""
Two checks that need the real stack, run through scripts/resilience_check.sh:

  conservation  money and shares across every account add up to the same totals after a run of trades
  recovery      after the engine is killed and restarted, orders that were resting in its book before the crash
                still match (the engine rebuilt its book by replaying the command log)

    resilience_check.py before   -> trade, check conservation, leave two resting orders, save their ids
    resilience_check.py after    -> (engine was killed and restarted in between) match the saved orders
"""
import json
import os
import sys
import threading
import time

import grpc
import redis
from confluent_kafka import OFFSET_END, Consumer, TopicPartition

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "ledger"))

import orders_pb2_grpc
from events_pb2 import ExchangeEvent
from orders_pb2 import ORDER_TYPE_FAK, ORDER_TYPE_GTC, SIDE_BUY, SIDE_SELL, SubmitOrder

GATEWAY_ADDR = os.getenv("GATEWAY_ADDR", "localhost:50051")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
STATE_FILE = os.getenv("RESILIENCE_STATE", "/tmp/resilience_state.json")
SYMBOL = "AAPL"
SELLER_KEY, BUYER_KEY = "mm-bot-key-111", "rt-bot-key-222"
ID_BASE = 950_000  # clear of every bot's block and the latency probe
PACE = 0.15

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
stub = orders_pb2_grpc.OrderServiceStub(grpc.insecure_channel(GATEWAY_ADDR))
fills = {}  # aggressor_id -> passive_id


def watch_fills():
    consumer = Consumer({"bootstrap.servers": KAFKA_BROKERS, "group.id": f"resilience-{os.getpid()}",
                         "enable.auto.commit": False})
    consumer.assign([TopicPartition(f"events.{SYMBOL}", 0, OFFSET_END)])
    while True:
        msg = consumer.poll(0.1)
        if msg is None or msg.error():
            continue
        event = ExchangeEvent()
        event.ParseFromString(msg.value())
        if event.HasField("trade_fill"):
            fills[event.trade_fill.aggressor_id] = event.trade_fill.passive_id


def place(key, order_id, side, price, qty, order_type):
    ack = stub.submit_order(
        SubmitOrder(api_key=key, symbol=SYMBOL, price=price, quantity=qty, order_direction=side,
                    order_type=order_type, client_order_id=order_id),
        metadata=[("x-api-key", key)], timeout=5)
    if not ack.accepted:
        raise SystemExit(f"order {order_id} rejected: {ack.rejected_reason}")
    time.sleep(PACE)


def totals():
    """Sums of cash and of each symbol's shares over every account, including what is reserved for open orders."""
    out = {"cash": 0.0}
    for pattern, label in (("balance:*:cash", "cash"), ("reserved:*:cash", "cash")):
        for key in r.scan_iter(pattern):
            out[label] += float(r.get(key) or 0)
    for pattern in ("balance:*:position:*", "reserved:*:position:*"):
        for key in r.scan_iter(pattern):
            symbol = key.decode().rsplit(":", 1)[1]
            out[symbol] = out.get(symbol, 0.0) + float(r.get(key) or 0)
    return out


def settled_totals():
    """Totals once the ledger has stopped changing them."""
    previous, stable = None, 0
    while stable < 4:
        time.sleep(0.5)
        current = totals()
        stable = stable + 1 if current == previous else 0
        previous = current
    return previous


def wait_for_fill(aggressor_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if aggressor_id in fills:
            return fills[aggressor_id]
        time.sleep(0.05)
    return None


def before():
    while not r.exists("balance:3:cash"):
        time.sleep(0.5)
    start = totals()
    print("totals before trading:", start)

    # a run of crossing trades between two accounts, some filling at a better price than the limit
    index = 0
    for round_ in range(40):
        sell_id, buy_id = ID_BASE + 2 * index, ID_BASE + 2 * index + 1
        index += 1
        place(SELLER_KEY, sell_id, SIDE_SELL, 150 + round_ % 3, 3, ORDER_TYPE_GTC)
        place(BUYER_KEY, buy_id, SIDE_BUY, 153, 5, ORDER_TYPE_FAK)  # buys 3, the other 2 are cancelled
    after_run = settled_totals()
    print("totals after 40 trades and cancelled remainders:", after_run)
    conserved = after_run == start
    print("CONSERVATION:", "PASS" if conserved else "FAIL")

    # leave one resting order on each side, far from the trading range, then remember them
    sell_id, buy_id = ID_BASE + 5000, ID_BASE + 5001
    place(SELLER_KEY, sell_id, SIDE_SELL, 158, 5, ORDER_TYPE_GTC)
    place(BUYER_KEY, buy_id, SIDE_BUY, 142, 5, ORDER_TYPE_GTC)
    json.dump({"sell_id": sell_id, "buy_id": buy_id, "totals": settled_totals()}, open(STATE_FILE, "w"))
    print("left resting orders", sell_id, "(sell 158) and", buy_id, "(buy 142)")
    return conserved


def after():
    state = json.load(open(STATE_FILE))

    # the restarted engine joins its consumer group a moment after it starts: trade at a clear price until it answers
    probe = ID_BASE + 6000
    deadline = time.time() + 90
    while time.time() < deadline:
        place(SELLER_KEY, probe, SIDE_SELL, 150, 1, ORDER_TYPE_GTC)
        place(BUYER_KEY, probe + 1, SIDE_BUY, 150, 1, ORDER_TYPE_FAK)
        if wait_for_fill(probe + 1, 1.5):
            break
        probe += 2
    else:
        raise SystemExit("the restarted engine never answered")

    # a buy that crosses the sell left resting before the crash, and a sell that crosses the resting buy
    buy_id, sell_id = ID_BASE + 7000, ID_BASE + 7001
    place(BUYER_KEY, buy_id, SIDE_BUY, 158, 5, ORDER_TYPE_FAK)
    place(SELLER_KEY, sell_id, SIDE_SELL, 142, 5, ORDER_TYPE_FAK)
    hit_sell = wait_for_fill(buy_id)
    hit_buy = wait_for_fill(sell_id)
    recovered = hit_sell == state["sell_id"] and hit_buy == state["buy_id"]
    print(f"buy matched the pre-crash sell {state['sell_id']}: {hit_sell == state['sell_id']}")
    print(f"sell matched the pre-crash buy {state['buy_id']}: {hit_buy == state['buy_id']}")
    print("RECOVERY:", "PASS" if recovered else "FAIL")

    final = settled_totals()
    print("totals after recovery:", final)
    conserved = final == state["totals"] or all(abs(final[k] - state["totals"][k]) < 1e-6 for k in final)
    print("CONSERVATION AFTER CRASH:", "PASS" if conserved else "FAIL")
    return recovered and conserved


if __name__ == "__main__":
    threading.Thread(target=watch_fills, daemon=True).start()
    time.sleep(2)
    ok = before() if sys.argv[1] == "before" else after()
    sys.exit(0 if ok else 1)
