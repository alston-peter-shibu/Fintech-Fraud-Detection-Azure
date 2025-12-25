# Real-Time FinTech Fraud Detection Pipeline on Azure

A production-style **real-time streaming data platform** for detecting suspicious financial transactions using **Apache Kafka, Azure Databricks (Spark Structured Streaming), Delta Lake on ADLS Gen2, Azure SQL Database, Azure Functions, and Power BI**.

The system simulates live transaction traffic, performs streaming fraud detection, persists data across multiple analytical layers, triggers automated alerts, and exposes curated datasets for reporting.

---

## Architecture Overview

Local Python Transaction Generator
        ↓
       Kafka (Azure VM)
        ↓
Azure Databricks (Spark Structured Streaming + Fraud Detection)
        ↓
───────────────────────────────
| Persist Data & Streams       |
|                              |
| ADLS Gen2                    |
| ├── Raw         (all transactions) 
| ├── Processed   (cleaned/enriched transactions)
| └── Fraud       (flagged suspicious transactions)
|                  │
|                  ├──────────→ Azure SQL Database
|                  │            (fraud alerts)
|                  └──────────→ Azure Function
|                               (SendGrid Email Alerts)
───────────────────────────────
        ↓
   Power BI Dashboard
   (reads from SQL Database)


---

## System Characteristics

- Event-driven, streaming-first architecture
- Exactly-once processing using Spark checkpointing
- Cloud-native storage with Delta Lake
- Clear separation of raw, processed, and fraud data layers
- Operational and analytical consumption paths
- Extensible fraud detection logic

---

## Technology Stack

| Layer           | Technology                                |
|-----------------|-------------------------------------------|
| Ingestion       | Apache Kafka                              |
| Processing      | Azure Databricks, PySpark                 |
| Storage         | Azure Data Lake Storage Gen2 (Delta Lake) |
| Database        | Azure SQL Database                        |
| Alerting        | Azure Functions (Python), SendGrid        |
| Analytics       | Power BI                                  |
| Infrastructure  | Azure VM, Azure CLI                       |
| Language        | Python                                    |

---

## Project Structure

```
azure-fraud-detection/
│
├── data-generator/
│   ├── stream_transactions.py
│   └── requirements.txt
│
├── databricks/
│   ├── notebooks/
│   │   ├── 01_mount_adls.py
│   │   └── 02_fraud_streaming_job.py
│   └── README.md
│
├── sql/
│   └── create_fraud_table.sql
│
├── azure-function/
│   └── fraud_email_function/
│       ├── function_app.py
│       ├── function.json
│       └── requirements.txt
│
├── architecture/
│   └── azure_fraud_pipeline.png
│
├── dashboard/
│   └── fraud_analytics.pbix
│
├── screenshots/
│   ├── architecture.png
│   ├── databricks_streaming_job.png
│   ├── sql_fraud_table.png
│   ├── email_alert_sample.png
│   └── powerbi_dashboard.png
│
└── README.md
```

---

## Data Flow

### 1. Transaction Ingestion
A Python-based generator produces synthetic financial transactions and publishes events to a Kafka topic (`transactions`).

### 2. Streaming Processing
Azure Databricks consumes Kafka events using Spark Structured Streaming. JSON payloads are parsed into structured DataFrames.

### 3. Fraud Detection
Rule-based fraud detection logic is applied in-stream. Transactions meeting fraud criteria are flagged and routed separately.

### 4. Storage Layers
- **Raw**: Immutable ingestion data
- **Processed**: Parsed and enriched records
- **Fraud**: Filtered suspicious transactions

All layers are stored as **Delta Lake tables written to ADLS Gen2 containers**.

### 5. Downstream Consumers
- Fraud records are written to **Azure SQL Database** for reporting
- Fraud batches trigger an **Azure Function** that sends email alerts
- Power BI connects to SQL for visualization

---

## Fraud Detection Logic (Example)

```python
when(amount > 2000) → FRAUD
when(merchant_country != "UK" AND amount > 1000) → FRAUD
```

The design allows easy replacement or extension with ML-based scoring models.

## Alerting Mechanism

- Implemented using Azure Functions (Python)
- Triggered per micro-batch of detected fraud
- Uses SendGrid API for email delivery
- Secrets managed via Azure Function configuration

---

## Power BI Dashboard

Power BI connects directly to **Azure SQL Database** and provides:

- Total fraud transaction count
- Fraud distribution by merchant country
- Transaction-level fraud details with timestamps

The dashboard is designed for operational monitoring and analysis.

---

## Technical Highlights

- Implemented real-time ingestion using Apache Kafka on Azure VM
- Built fault-tolerant Spark Structured Streaming pipelines with checkpointing
- Designed multi-layer Delta Lake storage architecture on ADLS Gen2
- Persisted Delta Lake tables automatically via Spark streaming writes
- Integrated streaming outputs with Azure SQL Database using JDBC
- Implemented event-driven alerting using Azure Functions and SendGrid
- Enabled downstream analytics using Power BI
- Deployed and configured Azure infrastructure including VM networking and IAM roles

---

## Execution (High-Level)

1. Start Kafka broker and topic on Azure VM
2. Run Python transaction generator
3. Start Databricks streaming job
4. Verify Delta Lake tables written to ADLS Gen2 containers
5. Confirm inserts in Azure SQL Database
6. Validate email alerts
7. Open Power BI dashboard

---

## Design Considerations

- Kafka decouples producers and consumers for scalability
- Delta Lake provides ACID guarantees on object storage
- SQL Database serves as a curated, query-optimized sink
- Azure Functions enable asynchronous, event-driven alerting
- Clear separation of compute, storage, and serving layers

## License

This project is provided for educational and demonstration purposes.

---

## Author

**N S Alston Peter**
Data Engineering | Streaming Systems | Azure
