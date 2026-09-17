from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    when
)


def transform_data(df):

    # Clean text fields
    df = df.withColumn("invoice_no", trim(col("invoice_no")))
    df = df.withColumn("stock_code", trim(col("stock_code")))
    df = df.withColumn("description", trim(col("description")))
    df = df.withColumn("country", trim(col("country")))

    # Convert data types
    df = df.withColumn(
        "quantity",
        col("quantity").cast("integer")
    )

    df = df.withColumn(
        "unit_price",
        col("unit_price").cast("double")
    )

    df = df.withColumn(
        "invoice_date",
        to_timestamp(col("invoice_date"))
    )

    # Remove records missing required fields
    df = df.filter(
        col("invoice_no").isNotNull()
        & col("stock_code").isNotNull()
        & col("quantity").isNotNull()
        & col("unit_price").isNotNull()
        & col("invoice_date").isNotNull()
    )

    # Remove invalid prices
    df = df.filter(col("unit_price") >= 0)

    # Classify transactions
    df = df.withColumn(
        "transaction_type",
        when(
            col("invoice_no").startswith("C")
            | (col("quantity") < 0),
            "RETURN"
        ).otherwise("SALE")
    )

    # Calculate revenue
    df = df.withColumn(
        "revenue",
        col("quantity") * col("unit_price")
    )

    # Remove rows with invalid revenue
    df = df.filter(col("revenue").isNotNull())

    # Remove rows with invalid transaction type
    df = df.filter(col("transaction_type").isNotNull())

    return df