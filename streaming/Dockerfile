FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir psycopg2-binary python-dotenv

WORKDIR /app

COPY spark_stream.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--master", "local[*]", \
     "/app/spark_stream.py"]
