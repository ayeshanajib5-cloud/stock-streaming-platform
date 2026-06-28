from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def calculate_daily_summary():
    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        dbname="stockdb",
        user="stockuser",
        password="stockpass"
    )
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO daily_summary (symbol, summary_date, high_price, low_price, avg_price, total_events)
        SELECT
            symbol,
            CURRENT_DATE,
            MAX(price),
            MIN(price),
            AVG(price),
            COUNT(*)
        FROM stock_prices
        WHERE created_at::date = CURRENT_DATE
        GROUP BY symbol
        ON CONFLICT (symbol, summary_date)
        DO UPDATE SET
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            avg_price = EXCLUDED.avg_price,
            total_events = EXCLUDED.total_events,
            created_at = NOW()
    """)

    conn.commit()
    rows_affected = cur.rowcount
    cur.close()
    conn.close()

    print(f"Daily summary calculated for {rows_affected} symbols")

default_args = {
    "owner": "ayesha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_stock_summary",
    default_args=default_args,
    description="Calculates daily high, low, and average price per stock",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["stock-streaming"],
) as dag:

    summary_task = PythonOperator(
        task_id="calculate_daily_summary",
        python_callable=calculate_daily_summary,
    )
