# Databricks notebook source
import random
import builtins
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import Row

raw_path = "/Volumes/workspace/default/fraud_project/raw/transactions"

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("city", StringType()),
    StructField("timestamp", TimestampType())
])

data = [
    Row(
        transaction_id=f"TX{random.randint(1000,9999)}",
        user_id=f"U{random.randint(1,50)}",
        amount=float(builtins.round(random.uniform(100,20000),2)),
        merchant=random.choice(["Amazon","Flipkart","Swiggy","Uber"]),
        city=random.choice(["Chennai","Mumbai","Delhi","Bangalore"]),
        timestamp=datetime.now()
    )
    for _ in range(100)
]

df = spark.createDataFrame(data, schema)

df.write.mode("append").json(raw_path)
