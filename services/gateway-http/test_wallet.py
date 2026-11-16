"""Run with: python3 test_wallet.py"""
import fnmatch
import wallet


class FakeRedis:
    def __init__(self):
        self.kv, self.hashes, self.zsets = {}, {}, {}

    def hgetall(self, k): return {a.encode(): str(b).encode() for a, b in self.hashes.get(k, {}).items()}
    def hset(self, k, f, v): self.hashes.setdefault(k, {})[f] = v
    def hincrbyfloat(self, k, f, a): self.hashes.setdefault(k, {})[f] = float(self.hashes.get(k, {}).get(f, 0)) + a
    def exists(self, k): return k in self.hashes or k in self.kv
    def expire(self, *_): pass
    def set(self, k, v, ex=None): self.kv[k] = v
    def get(self, k): return self.kv.get(k)
    def zadd(self, k, m): self.zsets.setdefault(k, {}).update(m)
    def zremrangebyrank(self, *_): pass


def new_order(side, qty, price):
    return {"symbol": "AAPL", "side": side, "qty": qty, "price": price, "sent_ms": 1}


def test_new_visitor_starts_with_cash_only():
    r = FakeRedis()
    assert wallet.view(r, "a") == {"cash": 100_000, "reserved": 0, "positions": {}}


def test_buy_fills_move_cash_and_shares():
    r = FakeRedis()
    wallet.open_order(r, "a", 1, new_order("buy", 10, 152))
    wallet.settle(r, "a", 1, [{"price": 151, "qty": 3}, {"price": 152, "qty": 7}])
    assert wallet.view(r, "a") == {"cash": 100_000 - 453 - 1064, "reserved": 0, "positions": {"AAPL": 10}}


def test_settling_twice_does_not_double_count():
    r = FakeRedis()
    wallet.open_order(r, "a", 1, new_order("buy", 5, 150))
    fills = [{"price": 150, "qty": 5}]
    assert wallet.settle(r, "a", 1, fills) == 1
    assert wallet.settle(r, "a", 1, fills) == 0
    assert wallet.view(r, "a")["cash"] == 100_000 - 750


def test_partial_then_rest_applies_only_new_fills():
    r = FakeRedis()
    wallet.open_order(r, "a", 1, new_order("buy", 10, 150))
    wallet.settle(r, "a", 1, [{"price": 150, "qty": 4}])
    wallet.settle(r, "a", 1, [{"price": 150, "qty": 4}, {"price": 150, "qty": 6}])
    assert wallet.view(r, "a")["positions"] == {"AAPL": 10}


def test_sell_needs_shares_and_buy_needs_cash():
    r = FakeRedis()
    assert wallet.check_funds(r, "a", "sell", "AAPL", 1, 150) == "insufficient_shares"
    assert wallet.check_funds(r, "a", "buy", "AAPL", 1000, 150) == "insufficient_funds"
    assert wallet.check_funds(r, "a", "buy", "AAPL", 100, 150) is None
    wallet.open_order(r, "a", 1, new_order("buy", 5, 150))
    wallet.settle(r, "a", 1, [{"price": 150, "qty": 5}])
    assert wallet.check_funds(r, "a", "sell", "AAPL", 5, 150) is None


def test_visitors_are_separate():
    r = FakeRedis()
    wallet.open_order(r, "a", 1, new_order("buy", 5, 150))
    wallet.settle(r, "a", 1, [{"price": 150, "qty": 5}])
    assert wallet.view(r, "b") == {"cash": 100_000, "reserved": 0, "positions": {}}


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
