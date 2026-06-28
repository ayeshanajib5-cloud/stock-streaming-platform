import os
import time
import json
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
STOCKS = ["AAPL", "TSLA", "MSFT"]

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_stock_price(symbol):
    url = "https://finnhub.io/api/v1/quote"
    params = {
        "symbol": symbol,
        "token": API_KEY
    }
    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    while True:
        for stock in STOCKS:
            data = get_stock_price(stock)
            event = {
                "symbol": stock,
                "price": data.get("c"),
                "change": data.get("d"),
                "change_percent": data.get("dp"),
                "timestamp": data.get("t")
            }
            producer.send("stock-prices", value=event)
            print("Sent:", event)
            time.sleep(1)
        time.sleep(10)
