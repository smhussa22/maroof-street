import time

# how long an order's metadata outlives its last fill or release; market-data still reads it to name the traders
META_GRACE_SECS = 60


def settle_balances(r, symbol, fill):
    # each side settles from its own order's metadata; a buyer reserved cash at their limit price,
    # so a fill at a better price gives the difference back
    for order_id in (fill.aggressor_id, fill.passive_id):
        key = f"order_meta:{symbol}:{order_id}"
        meta = r.hgetall(key)
        if not meta:
            continue

        account = meta[b"account_id"].decode()
        if meta[b"side"] == b"buy":
            limit = int(meta[b"price"])
            r.incrbyfloat(f"reserved:{account}:cash", -(limit * fill.quantity))
            r.incrbyfloat(f"balance:{account}:cash", (limit - fill.price) * fill.quantity)
            r.incrbyfloat(f"balance:{account}:position:{symbol}", fill.quantity)
        else:
            r.incrbyfloat(f"reserved:{account}:position:{symbol}", -fill.quantity)
            r.incrbyfloat(f"balance:{account}:cash", fill.price * fill.quantity)

        if r.hincrby(key, "remaining", -fill.quantity) <= 0:
            r.expire(key, META_GRACE_SECS)


def release_remaining(r, symbol, order_id):
    # gives back whatever part of the order's reservation is still held; safe to call twice
    key = f"order_meta:{symbol}:{order_id}"
    meta = r.hgetall(key)
    if not meta:
        return

    remaining = int(meta.get(b"remaining", 0))
    if remaining <= 0:
        return

    account = meta[b"account_id"].decode()
    if meta[b"side"] == b"buy":
        amount = int(meta[b"price"]) * remaining
        r.incrbyfloat(f"reserved:{account}:cash", -amount)
        r.incrbyfloat(f"balance:{account}:cash", amount)
    else:
        r.incrbyfloat(f"reserved:{account}:position:{symbol}", -remaining)
        r.incrbyfloat(f"balance:{account}:position:{symbol}", remaining)

    r.hset(key, "remaining", 0)
    r.expire(key, META_GRACE_SECS)


def expire_fak(r, now_ms=None):
    # the engine never reports the unfilled remainder of a FAK order, so the gateway queues each one
    # in fak_pending with a deadline and the ledger releases what is left once the deadline has passed
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    for member in r.zrangebyscore("fak_pending", "-inf", now_ms):
        symbol, order_id = member.decode().split(":")
        release_remaining(r, symbol, int(order_id))
        r.zrem("fak_pending", member)
