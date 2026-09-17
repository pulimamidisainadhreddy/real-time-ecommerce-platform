import os
import psycopg2

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from transformations import transform_data
from data_quality import check_data_quality


# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv()


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "online_retail"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5433"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "online_retail"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "postgres"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "root"
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "C:/temp/online_retail_checkpoint"
)

POSTGRES_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

POSTGRES_TABLE = "online_retail"
STAGING_TABLE = "online_retail_staging"


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OnlineRetailStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Kafka message schema
# --------------------------------------------------

schema = StructType([
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True)
])


# --------------------------------------------------
# Read data from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)


# --------------------------------------------------
# Convert Kafka value from JSON
# --------------------------------------------------

json_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# Transform data
# --------------------------------------------------

transformed_df = transform_data(json_df)


# --------------------------------------------------
# Write each micro-batch to PostgreSQL
# --------------------------------------------------

def write_to_postgres(batch_df, batch_id):

    print(f"Writing batch {batch_id} to PostgreSQL...")

    row_count = batch_df.count()

    quality_results = check_data_quality(batch_df)

    print(f"Data quality: {quality_results}")

    print(f"Batch {batch_id} contains {row_count} rows.")

    if row_count == 0:
        print(f"Batch {batch_id} is empty.")
        return

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cursor = connection.cursor()

    try:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.online_retail_staging (
                invoice_no VARCHAR(50),
                stock_code VARCHAR(50),
                description TEXT,
                quantity INTEGER,
                invoice_date TIMESTAMP,
                unit_price DOUBLE PRECISION,
                customer_id VARCHAR(50),
                country VARCHAR(100),
                transaction_type VARCHAR(20),
                revenue DOUBLE PRECISION
            );
        """)

        cursor.execute("""
            TRUNCATE TABLE public.online_retail_staging;
        """)

        connection.commit()

        (
            batch_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", STAGING_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

        cursor.execute("""
            INSERT INTO public.online_retail (
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
            FROM public.online_retail_staging
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
        """)

        connection.commit()

        print(f"Batch {batch_id} written successfully.")

    except Exception as e:

        connection.rollback()
        print(f"Batch {batch_id} failed: {e}")
        raise

    finally:

        cursor.close()
        connection.close()


# --------------------------------------------------
# Start streaming query
# --------------------------------------------------

query = (
    transformed_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .start()
)

query.awaitTermination()
