"""
Per-visitor demo wallets. Each website visitor has an anonymous id; their cash, shares and orders live in Redis,
separate from the exchange's own accounts. Every visitor order is sent to the engine through one shared, generously
funded exchange account, so the wallet is where one visitor's money is kept apart from another's.
"""
import json

START_CASH = 100_000
TTL_SECS = 60 * 60 * 24 * 30
ORDERS_KEPT = 50


def _wallet_key(visitor: str) -> str:
    return f"vw:{visitor}"


def _order_key(visitor: str, order_id: int) -> str:
    return f"vo:{visitor}:{order_id}"


def view(r, visitor: str) -> dict:
    h = r.hgetall(_wallet_key(visitor))
    get = lambda k, d=0: float(h.get(k.encode(), h.get(k, d)))
    positions = {k.decode().split(":", 1)[1]: int(float(v)) for k, v in h.items()
                 if k.decode().startswith("pos:") and int(float(v)) != 0}
    return {"cash": int(round(get("cash", START_CASH))), "reserved": 0, "positions": positions}


def check_funds(r, visitor: str, side: str, symbol: str, qty: int, price: int) -> str | None:
    """None if the visitor can afford the order, otherwise the reason it is refused."""
    w = view(r, visitor)
    if side == "buy" and w["cash"] < price * qty:
        return "insufficient_funds"
    if side == "sell" and w["positions"].get(symbol, 0) < qty:
        return "insufficient_shares"
    return None


def open_order(r, visitor: str, order_id: int, order: dict) -> None:
    key = _order_key(visitor, order_id)
    r.set(key, json.dumps({**order, "fills": []}), ex=TTL_SECS)
    r.zadd(f"vol:{visitor}", {str(order_id): order["sent_ms"]})
    # keep the list short; old orders expire with their TTL anyway
    r.zremrangebyrank(f"vol:{visitor}", 0, -(ORDERS_KEPT + 1))
    r.expire(f"vol:{visitor}", TTL_SECS)
    wallet = _wallet_key(visitor)
    if not r.exists(wallet):
        r.hset(wallet, "cash", START_CASH)
        r.expire(wallet, TTL_SECS)


def read_order(r, visitor: str, order_id: int) -> dict | None:
    raw = r.get(_order_key(visitor, order_id))
    return json.loads(raw) if raw else None


def settle(r, visitor: str, order_id: int, fills: list[dict], now_ms: int | None = None) -> int:
    """Applies fills not yet applied for this order and returns how many were new. Safe to call repeatedly with the
    full, oldest-first fill list: fills already applied are skipped."""
    order = read_order(r, visitor, order_id)
    if order is None:
        return 0
    new = fills[len(order["fills"]):]
    wallet = _wallet_key(visitor)
    sign = -1 if order["side"] == "buy" else 1
    for f in new:
        r.hincrbyfloat(wallet, "cash", sign * f["price"] * f["qty"])
        r.hincrbyfloat(wallet, f"pos:{order['symbol']}", -sign * f["qty"])
    if new:
        order["fills"] = order["fills"] + new
        if now_ms is not None:
            order["settled_ms"] = now_ms
        r.set(_order_key(visitor, order_id), json.dumps(order), ex=TTL_SECS)
        r.expire(wallet, TTL_SECS)
    return len(new)
