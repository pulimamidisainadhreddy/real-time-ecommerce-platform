from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    when,
    lit,
    round as spark_round,
    isnan,
)


# ============================================================
# Transaction classification
# ============================================================

def classify_transaction(invoice_no, quantity):
    """
    Classify a transaction for unit testing.
    """

    if invoice_no and str(invoice_no).upper().startswith("C"):
        return "CANCELLED"

    if quantity is not None and quantity < 0:
        return "RETURN"

    return "SALE"


# ============================================================
# Data transformation
# ============================================================

def transform_data(df: DataFrame) -> DataFrame:
    """
    Clean and transform valid Kafka records.

    Invalid JSON records are handled in streaming_job.py.
    This function handles data cleaning and business rules.
    """

    required_columns = [
        "invoice_no",
        "stock_code",
        "quantity",
        "unit_price",
        "invoice_date",
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )

    # ========================================================
    # Clean text columns
    # ========================================================

    text_columns = [
        "invoice_no",
        "stock_code",
        "description",
        "customer_id",
        "country",
    ]

    for column_name in text_columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                trim(col(column_name)),
            )

    df = df.withColumn(
        "invoice_no",
        upper(col("invoice_no")),
    )

    # ========================================================
    # Convert data types
    # ========================================================

    df = df.withColumn(
        "quantity",
        col("quantity").cast("integer"),
    )

    df = df.withColumn(
        "unit_price",
        col("unit_price").cast("double"),
    )

    df = df.withColumn(
        "invoice_date",
        to_timestamp(col("invoice_date")),
    )

    # ========================================================
    # Validate required fields
    # ========================================================

    for column_name in required_columns:
        df = df.filter(
            col(column_name).isNotNull()
        )

    # Remove empty invoice and stock codes

    df = df.filter(
        (col("invoice_no") != "")
        & (col("stock_code") != "")
    )

    # ========================================================
    # Validate numeric values
    # ========================================================

    df = df.filter(
        (~isnan(col("quantity")))
        & (~isnan(col("unit_price")))
    )

    # Unit price cannot be negative.
    # Quantity cannot be zero.
    df = df.filter(
        (col("unit_price") >= 0)
        & (col("quantity") != 0)
    )

    # ========================================================
    # Classify transactions
    # ========================================================

    df = df.withColumn(
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
            lit("SALE"),
        ),
    )

    # ========================================================
    # Calculate revenue
    # ========================================================

    df = df.withColumn(
        "revenue",
        spark_round(
            col("quantity") * col("unit_price"),
            2,
        ),
    )

    # ========================================================
    # Validate revenue
    # ========================================================

    df = df.filter(
        col("revenue").isNotNull()
    )

    # ========================================================
    # Remove duplicate business records
    # ========================================================

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

    available_duplicate_columns = [
        column_name
        for column_name in duplicate_columns
        if column_name in df.columns
    ]

    if available_duplicate_columns:
        df = df.dropDuplicates(
            available_duplicate_columns
        )

    return df