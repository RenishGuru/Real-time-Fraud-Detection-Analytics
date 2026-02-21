# Real-time-Fraud-Detection-Analytics
📌 Project Overview

This project implements a Fraud Analytics Data Platform using PySpark on Databricks following the Medallion Architecture (Bronze → Silver → Gold) pattern.

Building a scalable Lakehouse data pipeline
Cleaning and transforming large-scale transaction data
Designing curated fraud-ready datasets
Powering an interactive fraud monitoring dashboard

The goal was to simulate a real-world enterprise fraud analytics system.

🥉 Bronze Layer – Raw Data Ingestion
Ingested raw transaction CSV data into Delta tables
Schema enforcement
Stored immutable raw data
Enabled auditability

🥈 Silver Layer – Cleaned & Transformed Data
Removed nulls & inconsistencies
Standardized data formats
Derived fraud-related metrics
Deduplicated transactions

🥇 Gold Layer – Aggregated Business Metrics
Fraud rate by transaction type
Fraud trends over time
High-risk accounts
Region-wise fraud distribution
KPI-ready aggregated tables

⚙️ Tech Stack
Databricks
PySpark,
Spark SQL,
Delta Lake,
Lakehouse Architecture,
Dashboarding.

