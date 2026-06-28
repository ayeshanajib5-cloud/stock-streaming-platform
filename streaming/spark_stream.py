import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, when, abs as spark_abs
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("StockStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("change", DoubleType()) \
    .add("change_percent", DoubleType()) \
    .add("timestamp", LongType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

ALERT_THRESHOLD = 1.0

flagged_stream = parsed_stream.withColumn(
    "is_alert",
    when(spark_abs(col("change_percent")) >= ALERT_THRESHOLD, True).otherwise(False)
)

PG_URL = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:5432/stockdb"
PG_PROPERTIES = {
    "user": "stockuser",
    "password": "stockpass",
    "driver": "org.postgresql.Driver"
}

def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    prices_df = batch_df.select(
        "symbol", "price", "change", "change_percent",
        col("timestamp").alias("event_timestamp")
    )
    prices_df.write.jdbc(url=PG_URL, table="stock_prices", mode="append", properties=PG_PROPERTIES)

    alerts_df = batch_df.filter(col("is_alert") == True).select(
        "symbol", "price", "change_percent"
    ).withColumn(
        "alert_message",
        lit("Price moved more than threshold")
    )

    if not alerts_df.isEmpty():
        alerts_df.write.jdbc(url=PG_URL, table="stock_alerts", mode="append", properties=PG_PROPERTIES)

    print(f"Batch {batch_id}: wrote {prices_df.count()} prices, {alerts_df.count()} alerts")

query = flagged_stream.writeStream \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
