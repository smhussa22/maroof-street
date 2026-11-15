from market import price_band, summarize


def test_summarize_empty():
    s = summarize([])
    assert s["last"] is None and s["trades"] == 0 and s["spark"] == []


def test_summarize_orders_oldest_first_and_computes_change():
    newest_first = [{"price": 152, "qty": 5}, {"price": 150, "qty": 10}, {"price": 148, "qty": 1}]
    s = summarize(newest_first)
    assert s["last"] == 152
    assert s["change"] == 4
    assert s["change_pct"] == round(4 / 148 * 100, 2)
    assert (s["high"], s["low"]) == (152, 148)
    assert s["volume"] == 16
    assert s["spark"] == [148, 150, 152]


def test_summarize_caps_sparkline():
    trades = [{"price": p, "qty": 1} for p in range(100, 0, -1)]
    assert len(summarize(trades)["spark"]) == 40


def test_price_band():
    lo, hi = price_band(100)
    assert lo == 85 and hi == 116
    assert lo <= 100 < hi
