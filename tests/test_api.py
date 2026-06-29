from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.main import app, serialize_alert_row, serialize_price_row


def test_health_endpoint_returns_healthy_status():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_serialize_price_row_returns_api_shape():
    created_at = datetime(2026, 6, 29, 12, 30, tzinfo=timezone.utc)

    result = serialize_price_row(("AAPL", 212.45, 1.2, 0.57, created_at))

    assert result == {
        "symbol": "AAPL",
        "price": 212.45,
        "change": 1.2,
        "change_percent": 0.57,
        "created_at": "2026-06-29T12:30:00+00:00",
    }


def test_serialize_alert_row_returns_api_shape():
    created_at = datetime(2026, 6, 29, 12, 45, tzinfo=timezone.utc)

    result = serialize_alert_row(("TSLA", 330.10, -1.25, "Price moved more than threshold", created_at))

    assert result == {
        "symbol": "TSLA",
        "price": 330.10,
        "change_percent": -1.25,
        "alert_message": "Price moved more than threshold",
        "created_at": "2026-06-29T12:45:00+00:00",
    }
