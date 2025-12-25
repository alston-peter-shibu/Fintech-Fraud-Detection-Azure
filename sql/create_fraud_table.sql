-- Create a table to store fraud transactions
CREATE TABLE dbo.fraud_transactions (
    transaction_id NVARCHAR(50) NOT NULL PRIMARY KEY,
    customer_id NVARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    merchant_country NVARCHAR(50) NOT NULL,
    timestamp DATETIME2 NOT NULL,
    is_fraud BIT NOT NULL
);

-- Optional: Create an index for faster queries by customer
CREATE NONCLUSTERED INDEX idx_customer_id
ON dbo.fraud_transactions (customer_id);

-- Optional: Create an index for faster queries by timestamp
CREATE NONCLUSTERED INDEX idx_timestamp
ON dbo.fraud_transactions (timestamp);
