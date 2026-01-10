#!/bin/bash

exec spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  /app/spark_consumer.py