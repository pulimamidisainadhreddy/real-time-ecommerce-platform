import json
import os
import time
import uuid

import psycopg2
import pytest
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from pyspark.sql.functions import (
    col,
    concat_ws,
    from_json,
    lit,
    sha2,
    when,
)

import spark.streaming_job as streaming_job
from spark.transformations import transform_data


pytestmark = pytest.mark.integration


# ============================================================
# PostgreSQL connection
# ============================================================

def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "online_retail"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


# ============================================================
# Cleanup
# ============================================================

def cleanup_test_tables():
    connection = None

    try:
        connection = get_postgres_connection()
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                "DROP TABLE IF EXISTS "
                "public.test_online_retail_staging CASCADE"
            )

            cursor.execute(
                "DROP TABLE IF EXISTS "
                "public.test_online_retail CASCADE"
            )

    finally:
        if connection:
            connection.close()


# ============================================================
# Create test tables
# ============================================================

def create_test_tables():
    connection = get_postgres_connection()
    connection.autocommit = True

    try:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE public.test_online_retail_staging (
                    invoice_no VARCHAR(50),
                    stock_code VARCHAR(50),
                    description TEXT,
                    quantity INTEGER,
                    invoice_date TIMESTAMP,
                    unit_price NUMERIC(12, 2),
                    customer_id VARCHAR(50),
                    country VARCHAR(100),
                    transaction_type VARCHAR(20),
                    revenue NUMERIC(14, 2),
                    message_key VARCHAR(255),
                    kafka_topic VARCHAR(255),
                    kafka_partition INTEGER,
                    kafka_offset BIGINT,
                    kafka_timestamp TIMESTAMP,
                    record_hash VARCHAR(64)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE public.test_online_retail (
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

                    CONSTRAINT test_online_retail_unique_record
                    UNIQUE NULLS NOT DISTINCT (
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country
                    )
                )
                """
            )

    finally:
        connection.close()


# ============================================================
# Integration test
# ============================================================

def test_kafka_spark_postgres_integration(
    integration_spark,
):
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic = (
        f"online_retail_test_"
        f"{uuid.uuid4().hex[:10]}"
    )

    raw_invoice_no = (
        f"TEST-{uuid.uuid4().hex[:10]}"
    )

    # transform_data() converts invoice_no to uppercase.
    normalized_invoice_no = raw_invoice_no.upper()

    admin = None
    producer = None

    try:

        # ====================================================
        # 1. Kafka setup
        # ====================================================

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            request_timeout_ms=10000,
        )

        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            key_serializer=lambda key: key.encode(
                "utf-8"
            ),
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
        )

        admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1,
                )
            ]
        )

        # ====================================================
        # 2. Test event
        # ====================================================

        event = {
            "event_version": "1.0",
            "invoice_no": raw_invoice_no,
            "stock_code": "TEST-A1",
            "description": "Kafka integration product",
            "quantity": 3,
            "invoice_date": "2025-01-01 10:00:00",
            "unit_price": 15.0,
            "customer_id": "TEST-C1",
            "country": "Germany",
        }

        # ====================================================
        # 3. Producer → Kafka
        # ====================================================

        producer.send(
            topic,
            key=raw_invoice_no,
            value=event,
        ).get(timeout=10)

        producer.flush()

        time.sleep(2)

        # ====================================================
        # 4. Kafka → Spark
        # ====================================================

        kafka_df = (
            integration_spark.read
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                bootstrap,
            )
            .option(
                "subscribe",
                topic,
            )
            .option(
                "startingOffsets",
                "earliest",
            )
            .option(
                "endingOffsets",
                "latest",
            )
            .load()
        )

        parsed = (
            kafka_df
            .select(
                col("key")
                .cast("string")
                .alias("message_key"),

                col("value")
                .cast("string")
                .alias("message_value"),

                col("topic").alias(
                    "kafka_topic"
                ),

                col("partition").alias(
                    "kafka_partition"
                ),

                col("offset").alias(
                    "kafka_offset"
                ),

                col("timestamp").alias(
                    "kafka_timestamp"
                ),
            )
            .withColumn(
                "parsed_data",
                from_json(
                    col("message_value"),
                    streaming_job.retail_schema,
                ),
            )
            .filter(
                col("parsed_data").isNotNull()
            )
            .select(
                "message_key",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "message_value",
                "parsed_data.*",
            )
            .withColumn(
                "invoice_date",
                col("invoice_date").cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "transaction_type",
                when(
                    col("invoice_no").startswith("C"),
                    lit("CANCELLED"),
                )
                .when(
                    col("quantity") < 0,
                    lit("RETURN"),
                )
                .otherwise(
                    lit("SALE")
                ),
            )
            .withColumn(
                "revenue",
                col("quantity")
                * col("unit_price"),
            )
            .filter(
                col("invoice_no") == raw_invoice_no
            )
        )

        # Kafka → Spark
        assert parsed.count() == 1

        # ====================================================
        # 5. Spark transformation
        # ====================================================

        transformed = transform_data(parsed)

        assert transformed.count() == 1

        result = transformed.select(
            "invoice_no",
            "quantity",
            "unit_price",
            "revenue",
        ).collect()[0]

        assert result["invoice_no"] == normalized_invoice_no
        assert result["quantity"] == 3
        assert float(result["unit_price"]) == 15.0
        assert float(result["revenue"]) == 45.0

        # ====================================================
        # 6. PostgreSQL setup
        # ====================================================

        cleanup_test_tables()
        create_test_tables()

        # ====================================================
        # 7. Create record hash
        # ====================================================

        transformed = transformed.withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("invoice_no"),
                    col("stock_code"),
                    col("description"),
                    col("quantity").cast("string"),
                    col("invoice_date").cast("string"),
                    col("unit_price").cast("string"),
                    col("customer_id"),
                    col("country"),
                ),
                256,
            ),
        )

        staging_columns = [
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country",
            "transaction_type",
            "revenue",
            "message_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "record_hash",
        ]

        transformed = transformed.select(
            *staging_columns
        )

        # ====================================================
        # 8. Spark → PostgreSQL staging
        # ====================================================

        postgres_url = (
            "jdbc:postgresql://"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5433')}/"
            f"{os.getenv('POSTGRES_DB', 'online_retail')}"
        )

        (
            transformed.write
            .format("jdbc")
            .option(
                "url",
                postgres_url,
            )
            .option(
                "dbtable",
                "public.test_online_retail_staging",
            )
            .option(
                "user",
                os.getenv(
                    "POSTGRES_USER",
                    "postgres",
                ),
            )
            .option(
                "password",
                os.getenv(
                    "POSTGRES_PASSWORD",
                    "",
                ),
            )
            .option(
                "driver",
                "org.postgresql.Driver",
            )
            .option(
                "batchsize",
                "5000",
            )
            .mode("append")
            .save()
        )

        # ====================================================
        # 9. Verify staging
        # ====================================================

        connection = get_postgres_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        invoice_no,
                        quantity,
                        unit_price,
                        revenue
                    FROM public.test_online_retail_staging
                    WHERE invoice_no = %s
                    """,
                    (normalized_invoice_no,),
                )

                staging_row = cursor.fetchone()

        finally:
            connection.close()

        assert staging_row is not None
        assert staging_row[0] == normalized_invoice_no
        assert staging_row[1] == 3
        assert float(staging_row[2]) == 15.0
        assert float(staging_row[3]) == 45.0

        # ====================================================
        # 10. Staging → Final PostgreSQL
        # ====================================================

        connection = get_postgres_connection()
        connection.autocommit = True

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.test_online_retail (
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country,
                        transaction_type,
                        revenue,
                        record_hash
                    )
                    SELECT
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country,
                        transaction_type,
                        revenue,
                        record_hash
                    FROM public.test_online_retail_staging
                    ON CONFLICT (
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country
                    )
                    DO NOTHING
                    """
                )

                assert cursor.rowcount == 1

        finally:
            connection.close()

        # ====================================================
        # 11. Verify final table
        # ====================================================

        connection = get_postgres_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        invoice_no,
                        quantity,
                        unit_price,
                        revenue
                    FROM public.test_online_retail
                    WHERE invoice_no = %s
                    """,
                    (normalized_invoice_no,),
                )

                final_row = cursor.fetchone()

        finally:
            connection.close()

        assert final_row is not None
        assert final_row[0] == normalized_invoice_no
        assert final_row[1] == 3
        assert float(final_row[2]) == 15.0
        assert float(final_row[3]) == 45.0

        # ====================================================
        # 12. Duplicate handling
        # ====================================================

        connection = get_postgres_connection()
        connection.autocommit = True

        try:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO public.test_online_retail (
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country,
                        transaction_type,
                        revenue,
                        record_hash
                    )
                    SELECT
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country,
                        transaction_type,
                        revenue,
                        record_hash
                    FROM public.test_online_retail_staging
                    ON CONFLICT (
                        invoice_no,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        country
                    )
                    DO NOTHING
                    """
                )

                assert cursor.rowcount == 0

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM public.test_online_retail
                    WHERE invoice_no = %s
                    """,
                    (normalized_invoice_no,),
                )

                record_count = cursor.fetchone()[0]

        finally:
            connection.close()

        assert record_count == 1

    finally:

        # PostgreSQL cleanup
        try:
            cleanup_test_tables()
        except Exception:
            pass

        # Kafka cleanup
        if producer:
            producer.close()

        if admin:
            try:
                admin.delete_topics([topic])
            except Exception:
                pass

            admin.close()