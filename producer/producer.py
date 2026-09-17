import json
import logging

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DATA_FILE


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


df = pd.read_excel(DATA_FILE)

logger.info(f"Total records: {len(df)}")
logger.info("Starting producer...")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    batch_size=32768,
    linger_ms=10,
    compression_type="gzip",
    acks="all",
    retries=3
)


count = 0


for row in df.itertuples(index=False):

    # Validate required fields
    if pd.isna(row.InvoiceNo) or pd.isna(row.StockCode):
        logger.warning(f"Skipping invalid record: {count}")
        continue

    if pd.isna(row.Quantity) or pd.isna(row.UnitPrice):
        logger.warning(f"Skipping invalid record: {count}")
        continue

    event = {
        "invoice_no": None if pd.isna(row.InvoiceNo) else str(row.InvoiceNo),
        "stock_code": None if pd.isna(row.StockCode) else str(row.StockCode),
        "description": None if pd.isna(row.Description) else str(row.Description),
        "quantity": None if pd.isna(row.Quantity) else int(row.Quantity),
        "invoice_date": None if pd.isna(row.InvoiceDate) else str(row.InvoiceDate),
        "unit_price": None if pd.isna(row.UnitPrice) else float(row.UnitPrice),
        "customer_id": None if pd.isna(row.CustomerID) else str(row.CustomerID),
        "country": None if pd.isna(row.Country) else str(row.Country)
    }

    try:
        producer.send(KAFKA_TOPIC, value=event)
        count += 1

        if count % 10000 == 0:
            logger.info(f"Sent {count} records")

    except Exception as e:
        logger.error(f"Failed to send record {count}: {e}")


producer.flush()
producer.close()

logger.info(f"Producer finished. Total messages sent: {count}")
