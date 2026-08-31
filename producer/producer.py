import json
import time
import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


DATA_FILE = "data/sample/ecommerce_sample.csv"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


df = pd.read_csv(DATA_FILE)

print(f"Total records: {len(df)}")
print("Starting producer...")


for _, row in df.iterrows():

    event = {
        "invoice_no": str(row["InvoiceNo"]),
        "stock_code": str(row["StockCode"]),
        "description": str(row["Description"]),
        "quantity": int(row["Quantity"]),
        "invoice_date": str(row["InvoiceDate"]),
        "unit_price": float(row["UnitPrice"]),
        "customer_id": str(row["CustomerID"]),
        "country": str(row["Country"])
    }

    producer.send(KAFKA_TOPIC, value=event)

    print("Sent:", event)

    time.sleep(1)


producer.flush()
producer.close()

print("Producer finished.")

print(df.head())
print(df.shape)