from types import SimpleNamespace

from balances import expire_fak, release_remaining, settle_balances


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.zsets = {}

    def get(self, key):
        return self.values.get(key)

    def incrbyfloat(self, key, amount):
        self.values[key] = float(self.values.get(key, 0)) + amount

    def hgetall(self, key):
        return {k.encode(): str(v).encode() for k, v in self.hashes.get(key, {}).items()}

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    def hincrby(self, key, field, amount):
        h = self.hashes.setdefault(key, {})
        h[field] = int(h.get(field, 0)) + amount
        return h[field]

    def expire(self, key, secs):
        pass

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zrangebyscore(self, key, low, high):
        return [m.encode() for m, s in self.zsets.get(key, {}).items() if s <= high]

    def zrem(self, key, member):
        self.zsets.get(key, {}).pop(member.decode(), None)


def cash(r, account):
    return r.values.get(f"balance:{account}:cash", 0)


def reserved_cash(r, account):
    return r.values.get(f"reserved:{account}:cash", 0)


def shares(r, account, symbol="AAPL"):
    return r.values.get(f"balance:{account}:position:{symbol}", 0)


def reserved_shares(r, account, symbol="AAPL"):
    return r.values.get(f"reserved:{account}:position:{symbol}", 0)


def place(r, order_id, account, side, price, qty, symbol="AAPL"):
    # what the gateway does on submit: move the funds into reserved and write the order metadata
    if side == "buy":
        r.incrbyfloat(f"balance:{account}:cash", -price * qty)
        r.incrbyfloat(f"reserved:{account}:cash", price * qty)
    else:
        r.incrbyfloat(f"balance:{account}:position:{symbol}", -qty)
        r.incrbyfloat(f"reserved:{account}:position:{symbol}", qty)
    r.hashes[f"order_meta:{symbol}:{order_id}"] = {
        "account_id": account, "price": price, "qty": qty, "remaining": qty, "side": side,
    }


def fill(aggressor_id, passive_id, price, quantity):
    return SimpleNamespace(aggressor_id=aggressor_id, passive_id=passive_id, price=price, quantity=quantity)


def funded():
    r = FakeRedis()
    r.values.update({"balance:1:cash": 100000, "balance:2:cash": 100000})
    r.values.update({"balance:1:position:AAPL": 500, "balance:2:position:AAPL": 500})
    return r


def test_buyer_gets_price_improvement_back():
    r = funded()
    place(r, 1, 1, "buy", 155, 10)
    place(r, 2, 2, "sell", 153, 10)
    settle_balances(r, "AAPL", fill(1, 2, 153, 10))
    assert cash(r, 1) == 100000 - 153 * 10 and reserved_cash(r, 1) == 0
    assert shares(r, 1) == 510
    assert cash(r, 2) == 100000 + 153 * 10 and reserved_shares(r, 2) == 0
    assert shares(r, 2) == 490


def test_cancel_after_partial_fill_releases_only_the_remainder():
    r = funded()
    place(r, 1, 1, "buy", 148, 50)
    place(r, 2, 2, "sell", 148, 20)
    settle_balances(r, "AAPL", fill(2, 1, 148, 20))
    release_remaining(r, "AAPL", 1)
    assert cash(r, 1) == 100000 - 148 * 20 and reserved_cash(r, 1) == 0
    assert shares(r, 1) == 520


def test_release_is_idempotent():
    r = funded()
    place(r, 1, 1, "buy", 148, 50)
    release_remaining(r, "AAPL", 1)
    release_remaining(r, "AAPL", 1)
    assert cash(r, 1) == 100000 and reserved_cash(r, 1) == 0


def test_fak_remainder_is_released_after_its_deadline():
    r = funded()
    place(r, 1, 1, "buy", 154, 10)
    place(r, 2, 2, "sell", 153, 4)
    r.zadd("fak_pending", {"AAPL:1": 5000})
    settle_balances(r, "AAPL", fill(1, 2, 153, 4))

    expire_fak(r, now_ms=4999)
    assert reserved_cash(r, 1) == 154 * 6  # still inside the grace period

    expire_fak(r, now_ms=5000)
    assert reserved_cash(r, 1) == 0
    assert cash(r, 1) == 100000 - 153 * 4
    assert r.zsets["fak_pending"] == {}


def test_rejected_sell_returns_its_shares():
    r = funded()
    place(r, 1, 1, "sell", 150, 30)
    release_remaining(r, "AAPL", 1)
    assert shares(r, 1) == 500 and reserved_shares(r, 1) == 0


if __name__ == "__main__":
    names = [n for n in dir() if n.startswith("test_")]
    for name in names:
        globals()[name]()
    print("passed", names)
