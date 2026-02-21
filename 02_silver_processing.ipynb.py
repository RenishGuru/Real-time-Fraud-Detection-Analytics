# Databricks notebook source
from pyspark.sql.functions import col, when

bronze_path = "/Volumes/workspace/default/fraud_project/bronze/transactions"
silver_path = "/Volumes/workspace/default/fraud_project/silver/transactions"
checkpoint = "/Volumes/workspace/default/fraud_project/checkpoint/silver"

# Read Bronze streaming table
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# Silver transformation
silver_df = (
    bronze_df
    .dropDuplicates(["transaction_id"])
    .filter(col("amount").isNotNull())
    .withColumn(
        "fraud_flag",
        when(col("amount") > 15000, "High Risk").otherwise("Normal")
    )
)

# Write Silver stream
silver_stream = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .outputMode("append")
    .queryName("fraud_silver_stream")
    .trigger(availableNow=True)   
    .start(silver_path)
)
