import os
import time
import json
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
STOCKS = ["AAPL", "TSLA", "MSFT"]
STOCK_PRICES_TOPIC = os.getenv("STOCK_PRICES_TOPIC", "stock-prices")


def build_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def get_stock_price(symbol):
    if not API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is required")

    url = "https://finnhub.io/api/v1/quote"
    params = {
        "symbol": symbol,
        "token": API_KEY
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def build_stock_event(symbol, quote):
    return {
        "symbol": symbol,
        "price": quote.get("c"),
        "change": quote.get("d"),
        "change_percent": quote.get("dp"),
        "timestamp": quote.get("t")
    }


def run():
    producer = build_producer()
    while True:
        for stock in STOCKS:
            data = get_stock_price(stock)
            event = build_stock_event(stock, data)
            producer.send(STOCK_PRICES_TOPIC, value=event)
            print("Sent:", event)
            time.sleep(1)
        time.sleep(10)


if __name__ == "__main__":
    run()
