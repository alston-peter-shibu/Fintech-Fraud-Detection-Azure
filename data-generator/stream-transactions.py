import json
import random
import time
from kafka import KafkaProducer

# Replace with your Kafka broker endpoint
bootstrap_servers = ['<KAFKA_BROKER_IP>:<KAFKA_PORT>']
topic_name = '<KAFKA_TOPIC>'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize JSON to bytes
)

def generate_transaction():
    return {
        "transaction_id": str(random.randint(100000, 999999)),
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "amount": round(random.uniform(10, 5000), 2),
        "merchant_country": random.choice(["UK", "US", "DE", "FR", "IN"]),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

try:
    while True:
        transaction = generate_transaction()
        producer.send(topic_name, value=transaction)
        print(f"Sent transaction: {transaction}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer stopped by user.")
finally:
    producer.flush()
    producer.close()
