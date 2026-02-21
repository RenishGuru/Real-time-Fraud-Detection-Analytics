# Databricks notebook source
from pyspark.sql.types import *

raw_path = "/Volumes/workspace/default/fraud_project/raw/transactions"
bronze_path = "/Volumes/workspace/default/fraud_project/bronze/transactions"
checkpoint = "/Volumes/workspace/default/fraud_project/checkpoint/transactions"

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = (
    spark.readStream
    .schema(schema)
    .format("json")
    .load(raw_path)
)

bronze_stream = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)   
    .queryName("fraud_bronze_stream")
    .start(bronze_path)
)
