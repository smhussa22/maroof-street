"""
End-to-end latency probe: the time from a SubmitOrder call leaving the client to the matching fill event arriving
on events.AAPL. That covers the gateway (auth, rate limit, fund reservation), the Kafka command hop, the engine
match, and the Kafka event hop. Run it through scripts/latency_bench.sh, which brings up the minimal stack.
"""
import math
import os
import sys
import threading
import time

import grpc
from confluent_kafka import OFFSET_END, Consumer, TopicPartition

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "ledger"))

import orders_pb2_grpc
from events_pb2 import ExchangeEvent
from orders_pb2 import ORDER_TYPE_FAK, ORDER_TYPE_GTC, SIDE_BUY, SIDE_SELL, SubmitOrder

GATEWAY_ADDR = os.getenv("GATEWAY_ADDR", "localhost:50051")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
SAMPLES = int(os.getenv("SAMPLES", "1000"))
WARMUP = 50

SYMBOL = "AAPL"
PRICE = 150
SELLER_KEY = "mm-bot-key-111"
BUYER_KEY = "rt-bot-key-222"

# ids stay under the engine's 1,000,000 slot limit and clear of every bot's block
ID_BASE = 900_000

# each account sends one request per iteration, so 8 iterations a second stays under the gateway's 10/s limit
PACE_SECS = 0.125
FILL_TIMEOUT_SECS = 5

arrivals = {}
listener_ready = threading.Event()
stop_listening = threading.Event()


def listen_for_fills():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": f"latency-probe-{os.getpid()}",
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 5,
    })
    consumer.assign([TopicPartition(f"events.{SYMBOL}", 0, OFFSET_END)])
    listener_ready.set()

    while not stop_listening.is_set():
        message = consumer.poll(0.05)

        # stamp the arrival before parsing so decoding time is not counted
        arrived = time.perf_counter_ns()
        if message is None or message.error():
            continue

        event = ExchangeEvent()
        event.ParseFromString(message.value())
        if event.HasField("trade_fill"):
            arrivals[event.trade_fill.aggressor_id] = arrived

    consumer.close()


def place(stub, key, order_id, side, order_type):
    request = SubmitOrder(
        api_key=key, symbol=SYMBOL, price=PRICE, quantity=1,
        order_direction=side, order_type=order_type, client_order_id=order_id,
    )
    ack = stub.submit_order(request, metadata=[("x-api-key", key)], timeout=5)
    if not ack.accepted:
        raise RuntimeError(f"order {order_id} rejected: {ack.rejected_reason}")


def one_round(stub, index):
    # rest a sell, then time a crossing buy from the call until its fill event arrives
    sell_id = ID_BASE + 2 * index
    buy_id = sell_id + 1
    place(stub, SELLER_KEY, sell_id, SIDE_SELL, ORDER_TYPE_GTC)

    started = time.perf_counter_ns()
    place(stub, BUYER_KEY, buy_id, SIDE_BUY, ORDER_TYPE_FAK)
    acked = time.perf_counter_ns()

    deadline = time.time() + FILL_TIMEOUT_SECS
    while buy_id not in arrivals:
        if time.time() > deadline:
            return None
        time.sleep(0.0002)

    return acked - started, arrivals[buy_id] - started


def percentile(sorted_values, p):
    return sorted_values[max(0, math.ceil(p / 100 * len(sorted_values)) - 1)]


def report(label, values_ns):
    values_ms = sorted(v / 1e6 for v in values_ns)
    print(f"{label:<22} p50 {percentile(values_ms, 50):7.2f}  p90 {percentile(values_ms, 90):7.2f}  "
          f"p99 {percentile(values_ms, 99):7.2f}  max {values_ms[-1]:7.2f}   (ms)")


def main():
    threading.Thread(target=listen_for_fills, daemon=True).start()
    listener_ready.wait()
    time.sleep(2)

    stub = orders_pb2_grpc.OrderServiceStub(grpc.insecure_channel(GATEWAY_ADDR))

    # the engine joins its consumer group a moment after it starts, so keep trading until fills appear
    index = 0
    warm = 0
    deadline = time.time() + 60
    while warm < WARMUP:
        if time.time() > deadline:
            sys.exit("no fills after 60s: is the engine-host running with SYMBOL=AAPL?")
        loop_start = time.time()
        if one_round(stub, index) is not None:
            warm += 1
        index += 1
        time.sleep(max(0, PACE_SECS - (time.time() - loop_start)))

    # measured rounds
    acks = []
    fills = []
    missed = 0
    for _ in range(SAMPLES):
        loop_start = time.time()
        result = one_round(stub, index)
        index += 1
        if result is None:
            missed += 1
        else:
            acks.append(result[0])
            fills.append(result[1])
        time.sleep(max(0, PACE_SECS - (time.time() - loop_start)))

    stop_listening.set()
    print(f"\nlatency over {len(fills)} orders ({missed} without a fill), one machine, loopback")
    report("gateway ack", acks)
    report("order to fill event", fills)
    report("after the ack", [f - a for a, f in zip(acks, fills)])


if __name__ == "__main__":
    main()
