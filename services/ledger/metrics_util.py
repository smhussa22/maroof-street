"""Small Prometheus helpers shared by the Python services (this folder is on every service's PYTHONPATH)."""
import os

from prometheus_client import Gauge, start_http_server

LAG = Gauge("exchange_consumer_lag", "Events waiting to be read by this service's Kafka consumer", ["service"])


def serve_metrics(default_port: int) -> None:
    start_http_server(int(os.getenv("METRICS_PORT", default_port)))


def consumer_lag(consumer) -> int:
    """Events published to the consumer's partitions that it has not read yet."""
    total = 0
    for tp in consumer.assignment():
        low, high = consumer.get_watermark_offsets(tp, timeout=1, cached=True)
        position = consumer.position([tp])[0].offset
        total += max(0, high - (position if position >= 0 else low))
    return total
