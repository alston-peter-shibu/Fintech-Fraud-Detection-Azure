# Databricks Notebooks

This directory contains Databricks notebooks used for environment setup and real-time streaming processing.

---

## Notebook Overview

### 01_mount_adls.py
- Configures secure access to Azure Data Lake Storage Gen2
- Mounts required containers (`raw`, `processed`, `fraud`)
- Intended to be executed once per workspace or environment

### 02_fraud_streaming_job.py
- Consumes transaction events from Apache Kafka
- Applies real-time fraud detection logic using Spark Structured Streaming
- Writes streaming outputs as Delta Lake tables to ADLS Gen2
- Persists fraud records to Azure SQL Database
- Invokes an external Azure Function for email alerting

---

## Execution Notes

- Notebooks are intentionally separated to distinguish one-time environment setup from long-running streaming logic
- The streaming notebook is designed to run continuously as a Databricks job
- Checkpointing is enabled to ensure fault tolerance and exactly-once processing semantics
