REF_PRICES = {"AAPL": 150, "TSLA": 250, "NVDA": 400, "MSFT": 300, "GOOGL": 170}
BAND_PCT = 15
SPARK_POINTS = 40


def summarize(trades: list[dict]) -> dict:
    """Stats for one symbol from its tape, newest trade first."""
    if not trades:
        return {"last": None, "change": 0, "change_pct": 0.0, "high": None, "low": None,
                "volume": 0, "trades": 0, "spark": []}
    prices = [t["price"] for t in reversed(trades)]  # oldest first
    first, last = prices[0], prices[-1]
    return {
        "last":       last,
        "change":     last - first,
        "change_pct": round((last - first) / first * 100, 2) if first else 0.0,
        "high":       max(prices),
        "low":        min(prices),
        "volume":     sum(t["qty"] for t in trades),
        "trades":     len(trades),
        "spark":      prices[-SPARK_POINTS:],
    }


def price_band(reference: int) -> tuple[int, int]:
    return reference * (100 - BAND_PCT) // 100, reference * (100 + BAND_PCT) // 100 + 1
