import os
import uuid

import psycopg2
import pytest
from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    StructField,
    StructType,
)

TEST_FINAL_TABLE = "public.test_online_retail"
TEST_STAGING_TABLE = "public.test_online_retail_staging"
TEST_QUALITY_TABLE = "public.test_data_quality_results"
TEST_REJECTED_TABLE = "public.test_rejected_online_retail_records"

from spark.streaming_job import write_to_postgres  # noqa: E402


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_connection():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "online_retail"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_REJECTED_TABLE} CASCADE")
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_QUALITY_TABLE} CASCADE")
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_STAGING_TABLE} CASCADE")
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_FINAL_TABLE} CASCADE")

        cursor.execute(
            f"""
            CREATE TABLE {TEST_FINAL_TABLE} (
                invoice_no VARCHAR(50) NOT NULL,
                stock_code VARCHAR(50) NOT NULL,
                description TEXT,
                quantity INTEGER NOT NULL,
                invoice_date TIMESTAMP NOT NULL,
                unit_price NUMERIC(12, 2) NOT NULL,
                customer_id VARCHAR(50),
                country VARCHAR(100),
                transaction_type VARCHAR(20) NOT NULL,
                revenue NUMERIC(14, 2) NOT NULL,
                record_hash VARCHAR(64),
                CONSTRAINT test_online_retail_unique_record UNIQUE NULLS NOT DISTINCT (
                    invoice_no, stock_code, description, quantity,
                    invoice_date, unit_price, customer_id, country
                )
            )
            """
        )

    yield connection

    with connection.cursor() as cursor:
        for table in (
            TEST_REJECTED_TABLE,
            TEST_QUALITY_TABLE,
            TEST_STAGING_TABLE,
            TEST_FINAL_TABLE,
        ):
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    connection.close()


@pytest.fixture
def database_row():
    return (
        "TEST-DB-100",
        "TEST-A1",
        "Integration product",
        2,
        "2025-01-01 10:00:00",
        12.50,
        "TEST-C1",
        "Germany",
        "SALE",
        25.0,
        None,
        "online_retail_test",
        0,
        0,
        datetime(2025, 1, 1, 10, 0, 0),
    )


def test_database_insertion_and_duplicate_handling(
    integration_spark,
    postgres_connection,
    database_row,
):
    schema = StructType([
        StructField("invoice_no", StringType(), False),
        StructField("stock_code", StringType(), False),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("invoice_date", StringType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_type", StringType(), False),
        StructField("revenue", DoubleType(), False),
        StructField("message_key", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
    ])

    row = list(database_row)
    df = integration_spark.createDataFrame([tuple(row)], schema)

    batch_id = int(uuid.uuid4().int % 2_000_000_000)
    write_to_postgres(df, batch_id)

    # A second write of the same business record must be ignored.
    write_to_postgres(df, batch_id + 1)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {TEST_FINAL_TABLE} WHERE invoice_no = %s",
            (database_row[0],),
        )
        count = cursor.fetchone()[0]

    assert count == 1
