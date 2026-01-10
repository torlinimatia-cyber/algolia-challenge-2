FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY spark_consumer.py .
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

RUN mkdir -p /output/orders /tmp/spark-checkpoints /home/spark/.ivy2 && \
    chown -R spark:spark /output /tmp/spark-checkpoints /home/spark/.ivy2

USER spark

ENTRYPOINT ["/app/entrypoint.sh"]