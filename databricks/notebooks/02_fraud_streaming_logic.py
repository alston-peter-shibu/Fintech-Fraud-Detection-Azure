# ============================================================
# Cell 1 — Imports & Kafka Configuration
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka VM details
kafka_bootstrap_servers = "<KAFKA_VM_PUBLIC_IP>:9092"
kafka_topic = "transactions"  # Topic created on Kafka VM

# Kafka options for Databricks to VM connection
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest"  # Read messages from the beginning
}


# ============================================================
# Cell 2 — Read from Kafka & Parse JSON Messages
# ============================================================

# Read from Kafka VM
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Parse messages from 'value' (binary) to structured DataFrame
transactions = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("merchant_country", StringType()),
        StructField("timestamp", StringType())
    ])).alias("data")) \
    .select("data.*")

# Verify schema
transactions.printSchema()


# ============================================================
# Cell 3 — Fraud Detection Logic (Rule-Based)
# ============================================================

fraud = transactions.withColumn(
    "is_fraud",
    when(col("amount") > 2000, True)
    .when((col("merchant_country") != "UK") & (col("amount") > 1000), True)
    .otherwise(False)
).filter(col("is_fraud") == True)

# Memory sink for validation/testing
fraud.writeStream.format("memory").queryName("fraud_test").start()
spark.sql("SELECT * FROM fraud_test").show(5, truncate=False)


# ============================================================
# Cell 4 — Persist Streams to Delta Lake (ADLS Gen2)
# ============================================================

# Raw transactions
transactions.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/raw/checkpoint") \
    .start("/mnt/raw/")

# Processed transactions
transactions.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/processed/checkpoint") \
    .start("/mnt/processed/")

# Fraud transactions
fraud.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/fraud/checkpoint") \
    .start("/mnt/fraud/")


# ============================================================
# Cell 5 — Reset Streams & Storage (Development Only)
# ============================================================

for s in spark.streams.active:
    s.stop()

dbutils.fs.rm("/mnt/raw", recurse=True)
dbutils.fs.rm("/mnt/processed", recurse=True)
dbutils.fs.rm("/mnt/fraud", recurse=True)

dbutils.fs.mkdirs("/mnt/raw")
dbutils.fs.mkdirs("/mnt/processed")
dbutils.fs.mkdirs("/mnt/fraud")


# ============================================================
# Cell 6 — Write Fraud Transactions to Azure SQL Database
# ============================================================

# Azure SQL connection details
jdbc_url = "jdbc:sqlserver://<SQL_SERVER_NAME>.database.windows.net:1433;databaseName=<DATABASE_NAME>"

jdbc_properties = {
    "user": "<SQL_USERNAME>",
    "password": "<SQL_PASSWORD>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Function executed for each micro-batch
def write_fraud_to_sql(batch_df, batch_id):

    # Skip empty batches
    if batch_df.isEmpty():
        return

    # Convert timestamp to SQL-friendly type
    batch_df = batch_df.withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    )

    batch_df.write \
        .jdbc(
            url=jdbc_url,
            table="dbo.fraud_transactions",
            mode="append",
            properties=jdbc_properties
        )

# Start streaming write to SQL Server
fraud_sql_stream = fraud.writeStream \
    .foreachBatch(write_fraud_to_sql) \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/fraud/sql_checkpoint") \
    .start()


# ============================================================
# Cell 7 — Test Azure Function Email Alert (Manual Test)
# ============================================================

import requests

FUNCTION_URL = "<AZURE_FUNCTION_INVOKE_URL_WITH_KEY>"
MY_EMAIL = "<RECIPIENT_EMAIL_ADDRESS>"

payload = {
    "transaction_id": "TXTEST123",
    "customer_email": MY_EMAIL,
    "amount": 999.99
}

try:
    response = requests.post(FUNCTION_URL, json=payload)
    print("Response status:", response.status_code)
    print("Response body:", response.text)
except Exception as e:
    print("Failed to call function:", e)


# ============================================================
# Cell 8 — Send Email Alerts per Fraud Micro-Batch
# ============================================================

import requests

FUNCTION_URL = "<AZURE_FUNCTION_INVOKE_URL_WITH_KEY>"
MY_EMAIL = "<RECIPIENT_EMAIL_ADDRESS>"

def send_email_alert(batch_df, batch_id):
    """
    Sends a single email per micro-batch with
    all detected fraud transactions combined.
    """
    fraud_list = batch_df.select("transaction_id", "amount", "customer_id").collect()
    
    if not fraud_list:
        return

    transactions_str = "\n".join(
        [f"Transaction {row['transaction_id']} - Amount: ${row['amount']}" for row in fraud_list]
    )
    
    payload = {
        "transaction_id": "Multiple",
        "customer_email": MY_EMAIL,
        "amount": transactions_str
    }
    
    try:
        response = requests.post(FUNCTION_URL, json=payload)
        print(f"Batch {batch_id} email response:", response.status_code, response.text)
    except Exception as e:
        print(f"Batch {batch_id} failed to send email:", e)

# Attach to streaming query
fraud.writeStream.foreachBatch(send_email_alert).start()
