import os
import json
import psycopg2
import redis
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Stock Streaming Analytics API")

redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def serialize_price_row(row):
    return {
        "symbol": row[0],
        "price": row[1],
        "change": row[2],
        "change_percent": row[3],
        "created_at": row[4].isoformat()
    }


def serialize_alert_row(row):
    return {
        "symbol": row[0],
        "price": row[1],
        "change_percent": row[2],
        "alert_message": row[3],
        "created_at": row[4].isoformat()
    }


@app.get("/")
def root():
    return {"message": "Stock Streaming Analytics API is running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/prices/latest")
def latest_prices():
    cached = redis_client.get("latest_prices")
    if cached:
        return {"source": "cache", "data": json.loads(cached)}

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (symbol) symbol, price, change, change_percent, created_at
        FROM stock_prices
        ORDER BY symbol, created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        result.append(serialize_price_row(row))

    redis_client.setex("latest_prices", 10, json.dumps(result))

    return {"source": "database", "data": result}

@app.get("/stocks/{symbol}/history")
def stock_history(symbol: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, price, change, change_percent, created_at
        FROM stock_prices
        WHERE symbol = %s
        ORDER BY created_at DESC
        LIMIT 50
    """, (symbol.upper(),))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        result.append(serialize_price_row(row))
    return result

@app.get("/alerts/recent")
def recent_alerts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, price, change_percent, alert_message, created_at
        FROM stock_alerts
        ORDER BY created_at DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        result.append(serialize_alert_row(row))
    return result
