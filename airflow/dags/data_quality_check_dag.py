from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import psycopg2

FRESHNESS_THRESHOLD_MINUTES = 10

def check_data_freshness():
    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        dbname="stockdb",
        user="stockuser",
        password="stockpass"
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT symbol, MAX(created_at) as last_seen
        FROM stock_prices
        GROUP BY symbol
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise ValueError("No price data found in stock_prices table at all")

    stale_symbols = []
    now = datetime.now(timezone.utc)

    for symbol, last_seen in rows:
        minutes_since = (now - last_seen.replace(tzinfo=timezone.utc)).total_seconds() / 60
        if minutes_since > FRESHNESS_THRESHOLD_MINUTES:
            stale_symbols.append((symbol, round(minutes_since, 1)))

    if stale_symbols:
        details = ", ".join([f"{s} ({m} min old)" for s, m in stale_symbols])
        raise ValueError(f"Stale data detected for: {details}")

    print(f"Data freshness check passed for {len(rows)} symbols")

default_args = {
    "owner": "ayesha",
    "retries": 0,
}

with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="Checks that recent price data is actually flowing into the pipeline",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["stock-streaming", "data-quality"],
) as dag:

    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )
