# Databricks notebook source
from pyspark.sql.functions import sum, count
import time

silver_path = "/Volumes/workspace/default/fraud_project/silver/transactions"
gold_path = "/Volumes/workspace/default/fraud_project/gold/user_spending"

# Dynamic checkpoint version
checkpoint = f"/Volumes/workspace/default/fraud_project/checkpoint/transactions_{int(time.time())}"

# ✅ Always safe
dbutils.fs.mkdirs(checkpoint)

silver_df = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

gold_df = (
    silver_df
    .withWatermark("timestamp", "10 minutes")
    .groupBy("user_id")
    .agg(
        sum("amount").alias("total_spending"),
        count("*").alias("transaction_count")
    )
)

gold_stream = (
    gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .outputMode("complete")
    .trigger(availableNow=True)
    .queryName("fraud_gold_stream")
    .start(gold_path)
)
