import json
import logging
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    sha2,
    when,
    count,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

try:
    from .data_quality import check_data_quality
    from .transformations import transform_data
except ImportError:
    from data_quality import check_data_quality
    from transformations import transform_data


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("online_retail_streaming")


# ============================================================
# Configuration
# ============================================================

try:
    from .config import (
        CHECKPOINT_LOCATION,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_FAIL_ON_DATA_LOSS,
        KAFKA_STARTING_OFFSETS,
        KAFKA_TOPIC,
        MALFORMED_CHECKPOINT_LOCATION,
        MALFORMED_MESSAGES_FILE,
        POSTGRES_BATCH_SIZE,
        POSTGRES_DB,
        POSTGRES_DRIVER,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_URL,
        POSTGRES_USER,
        SPARK_APP_NAME,
        SPARK_LOG_LEVEL,
        SPARK_MASTER,
        SPARK_MAX_OFFSETS_PER_TRIGGER,
        SPARK_TIMEZONE,
        STAGING_TABLE,
        FINAL_TABLE,
        QUALITY_RESULTS_TABLE,
        REJECTED_RECORDS_TABLE,
    )
except ImportError:
    from config import (
        CHECKPOINT_LOCATION,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_FAIL_ON_DATA_LOSS,
        KAFKA_STARTING_OFFSETS,
        KAFKA_TOPIC,
        MALFORMED_CHECKPOINT_LOCATION,
        MALFORMED_MESSAGES_FILE,
        POSTGRES_BATCH_SIZE,
        POSTGRES_DB,
        POSTGRES_DRIVER,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_URL,
        POSTGRES_USER,
        SPARK_APP_NAME,
        SPARK_LOG_LEVEL,
        SPARK_MASTER,
        SPARK_MAX_OFFSETS_PER_TRIGGER,
        SPARK_TIMEZONE,
        STAGING_TABLE,
        FINAL_TABLE,
        QUALITY_RESULTS_TABLE,
        REJECTED_RECORDS_TABLE,
    )


# ============================================================
# Spark session and Kafka input
# ============================================================

retail_schema = StructType([
    StructField("event_version", StringType(), True),
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
])


def create_spark_session():
    """Create and configure the Spark session."""
    session = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return session


def build_stream_dataframes(spark_session):
    """Build Kafka input, valid JSON, malformed JSON and quality streams."""
    kafka_df = (
        spark_session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", str(KAFKA_FAIL_ON_DATA_LOSS).lower())
        .option("maxOffsetsPerTrigger", SPARK_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    kafka_messages_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = kafka_messages_df.withColumn(
        "parsed_data",
        from_json(col("message_value"), retail_schema),
    )

    valid_json_df = parsed_df.filter(col("parsed_data").isNotNull())
    malformed_json_df = parsed_df.filter(col("parsed_data").isNull())

    json_df = valid_json_df.select(
        "message_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "message_value",
        "parsed_data.*",
    )

    quality_input_df = (
        json_df
        .withColumn(
            "invoice_no",
            when(
                col("invoice_no").isNull()
                | (col("invoice_no").cast("string").isin("")),
                lit(None),
            ).otherwise(col("invoice_no").cast("string")),
        )
        .withColumn(
            "stock_code",
            when(
                col("stock_code").isNull()
                | (col("stock_code").cast("string").isin("")),
                lit(None),
            ).otherwise(col("stock_code").cast("string")),
        )
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("invoice_date", col("invoice_date").cast("timestamp"))
        .withColumn(
            "transaction_type",
            when(col("invoice_no").startswith("C"), lit("CANCELLED"))
            .when(col("quantity") < 0, lit("RETURN"))
            .otherwise(lit("SALE")),
        )
        .withColumn(
            "revenue",
            col("quantity") * col("unit_price"),
        )
    )

    return quality_input_df, malformed_json_df


# ============================================================
# Malformed-message handling
# ============================================================

def save_malformed_messages(batch_df, batch_id):
    """Save malformed Kafka records to a JSONL file."""

    malformed_count = batch_df.count()

    if malformed_count == 0:
        return

    logger.error(
        "Batch %s contains %s malformed Kafka messages.",
        batch_id,
        malformed_count,
    )

    records = (
        batch_df.select(
            "message_key",
            "message_value",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .toJSON()
        .collect()
    )

    with MALFORMED_MESSAGES_FILE.open("a", encoding="utf-8") as file:
        for record in records:
            malformed_record = json.loads(record)
            malformed_record["error_reason"] = (
                "Invalid JSON or incompatible Kafka schema"
            )
            malformed_record["batch_id"] = int(batch_id)
            file.write(
                json.dumps(malformed_record, ensure_ascii=False) + "\n"
            )


# ============================================================
# Data-quality persistence helpers
# ============================================================

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def create_quality_tables(cursor):
    """Create the data-quality result and rejected-record tables."""

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_RESULTS_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            batch_id BIGINT NOT NULL,
            quality_timestamp TIMESTAMPTZ NOT NULL,
            total_rows BIGINT,
            null_required_fields BIGINT,
            invalid_price BIGINT,
            invalid_quantity BIGINT,
            invalid_date BIGINT,
            invalid_revenue BIGINT,
            negative_revenue BIGINT,
            invalid_transaction_type BIGINT,
            duplicate_groups BIGINT,
            duplicate_rows BIGINT,
            duplicate_check_passed BOOLEAN,
            quality_check_passed BOOLEAN NOT NULL
        );
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {REJECTED_RECORDS_TABLE} (
            id BIGSERIAL PRIMARY KEY,
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
            record_hash VARCHAR(64),
            rejection_batch_id BIGINT NOT NULL,
            rejected_at TIMESTAMPTZ NOT NULL,
            rejection_reason TEXT NOT NULL
        );
        """
    )


def save_quality_results(quality_results, batch_id, cursor):
    """Save one quality report for each processed batch."""

    cursor.execute(
        f"""
        INSERT INTO {QUALITY_RESULTS_TABLE} (
            batch_id,
            quality_timestamp,
            total_rows,
            null_required_fields,
            invalid_price,
            invalid_quantity,
            invalid_date,
            invalid_revenue,
            negative_revenue,
            invalid_transaction_type,
            duplicate_groups,
            duplicate_rows,
            duplicate_check_passed,
            quality_check_passed
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        );
        """,
        (
            int(batch_id),
            quality_results["quality_timestamp"],
            quality_results["total_rows"],
            quality_results["null_required_fields"],
            quality_results["invalid_price"],
            quality_results["invalid_quantity"],
            quality_results["invalid_date"],
            quality_results["invalid_revenue"],
            quality_results["negative_revenue"],
            quality_results["invalid_transaction_type"],
            quality_results["duplicate_groups"],
            quality_results["duplicate_rows"],
            quality_results["duplicate_check_passed"],
            quality_results["quality_check_passed"],
        ),
    )


def build_rejected_records(batch_df, batch_id):
    """
    Build records that fail data-quality rules.

    Invalid records are rejected. Duplicate records are handled by
    transform_data() using dropDuplicates().
    """

    invalid_basic_condition = (
        col("invoice_no").isNull()
        | col("stock_code").isNull()
        | col("quantity").isNull()
        | (col("quantity") == 0)
        | col("unit_price").isNull()
        | (col("unit_price") < 0)
        | col("invoice_date").isNull()
        | col("revenue").isNull()
        | col("transaction_type").isNull()
        | ~col("transaction_type").isin(
            "SALE",
            "RETURN",
            "CANCELLED",
        )
    )

    duplicate_columns = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ]

    available_columns = [
        name for name in duplicate_columns if name in batch_df.columns
    ]

    if available_columns:
        duplicate_window = Window.partitionBy(*available_columns)
        duplicate_condition = count("*").over(duplicate_window) > 1
    else:
        duplicate_condition = lit(False)

    rejected_df = (
        batch_df
        .withColumn(
            "_is_rejected",
            invalid_basic_condition,
        )
        .filter(col("_is_rejected"))
        .drop("_is_rejected")
        .withColumn("rejection_batch_id", lit(int(batch_id)))
        .withColumn("rejected_at", current_timestamp())
        .withColumn(
            "rejection_reason",
            lit("Data-quality check failed"),
        )
    )

    return rejected_df


def save_rejected_records(batch_df, batch_id):
    """Persist rejected records to PostgreSQL."""

    rejected_df = build_rejected_records(batch_df, batch_id)

    if rejected_df.limit(1).count() == 0:
        logger.info("No rejected records found for batch %s.", batch_id)
        return

    rejected_df = rejected_df.withColumn(
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

    target_columns = [
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
        "rejection_batch_id",
        "rejected_at",
        "rejection_reason",
    ]

    available_target_columns = [
        name for name in target_columns if name in rejected_df.columns
    ]

    (
        rejected_df.select(*available_target_columns)
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", REJECTED_RECORDS_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("batchsize", str(POSTGRES_BATCH_SIZE))
        .mode("append")
        .save()
    )

    logger.error(
        "Saved rejected records for batch %s.",
        batch_id,
    )


# ============================================================
# PostgreSQL writer
# ============================================================

def write_to_postgres(batch_df, batch_id):
    """Validate, persist quality information, and write valid batches."""

    batch_count = batch_df.count()

    if batch_count == 0:
        logger.info("Batch %s is empty.", batch_id)
        return

    logger.info("Processing batch %s with %s records.", batch_id, batch_count)

    quality_results = check_data_quality(batch_df)

    logger.error("========== DATA QUALITY RESULTS ==========")
    for check_name, check_value in quality_results.items():
        logger.error("%s: %s", check_name, check_value)
    logger.error("==========================================")

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        create_quality_tables(cursor)
        save_quality_results(quality_results, batch_id, cursor)
        connection.commit()

        if not quality_results["quality_check_passed"]:
            logger.warning(
                "DATA-QUALITY FAILURE in batch %s.",
                batch_id,
            )

            save_rejected_records(batch_df, batch_id)

            logger.warning(
                "Rejected records were saved. "
                "The pipeline will continue with this batch. "
                "Negative revenue and duplicates are allowed and handled "
                "as business cases."
            )

        # Apply cleaning and duplicate removal only after quality checking.
        batch_to_write = transform_data(batch_df)

        batch_to_write = batch_to_write.withColumn(
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

        # Write ONLY columns that exist in the PostgreSQL staging table.
        # Kafka metadata such as message_value must not be sent to JDBC.
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

        batch_to_write = batch_to_write.select(*staging_columns)

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
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
            );
            """
        )

        connection.commit()

        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE};")
        connection.commit()

        (
            batch_to_write.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", STAGING_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", POSTGRES_DRIVER)
            .option("batchsize", "50000")
            .mode("append")
            .save()
        )

        cursor.execute(
            f"""
            INSERT INTO {FINAL_TABLE} (
                invoice_no,
                stock_code,
                description,
                quantity,
                invoice_date,
                unit_price,
                customer_id,
                country,
                transaction_type,
                revenue
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
                revenue
            FROM {STAGING_TABLE}
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
            DO NOTHING;
            """
        )

        inserted_rows = cursor.rowcount
        connection.commit()

        logger.info(
            "Batch %s completed. Inserted rows: %s",
            batch_id,
            inserted_rows,
        )

    except Exception:
        if connection:
            connection.rollback()

        logger.exception("Batch %s failed.", batch_id)
        raise

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# ============================================================
# Start streaming queries
# ============================================================

def main():
    """Start Kafka -> Spark -> PostgreSQL streaming queries."""
    spark_session = create_spark_session()
    quality_input_df, malformed_json_df = build_stream_dataframes(spark_session)

    logger.info("Starting Kafka -> Spark -> PostgreSQL pipeline.")
    logger.info("PostgreSQL JDBC batch size: 50000")
    logger.info("Processing mode: availableNow=True.")

    query = (
        quality_input_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )

    malformed_query = (
        malformed_json_df.writeStream
        .foreachBatch(save_malformed_messages)
        .outputMode("append")
        .option("checkpointLocation", MALFORMED_CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )

    try:
        query.awaitTermination()
        malformed_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        if query.isActive:
            query.stop()
        if malformed_query.isActive:
            malformed_query.stop()
        spark_session.stop()
        logger.info("Spark streaming job stopped.")


if __name__ == "__main__":
    main()
