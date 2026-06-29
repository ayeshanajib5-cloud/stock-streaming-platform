import pytest

from producer import producer
from producer.producer import build_stock_event, get_stock_price


def test_build_stock_event_maps_finnhub_quote_to_internal_event():
    quote = {
        "c": 212.45,
        "d": 1.2,
        "dp": 0.57,
        "t": 1782734400,
    }

    event = build_stock_event("AAPL", quote)

    assert event == {
        "symbol": "AAPL",
        "price": 212.45,
        "change": 1.2,
        "change_percent": 0.57,
        "timestamp": 1782734400,
    }


def test_get_stock_price_requires_api_key(monkeypatch):
    monkeypatch.setattr(producer, "API_KEY", None)

    with pytest.raises(RuntimeError, match="FINNHUB_API_KEY"):
        get_stock_price("AAPL")
