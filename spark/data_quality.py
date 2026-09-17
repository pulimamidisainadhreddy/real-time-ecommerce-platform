from pyspark.sql.functions import col


def check_data_quality(df):
    """
    Run basic data-quality checks on the transformed data.
    """

    total_rows = df.count()

    null_required = df.filter(
        col("invoice_no").isNull()
        | col("stock_code").isNull()
        | col("quantity").isNull()
        | col("unit_price").isNull()
        | col("invoice_date").isNull()
    ).count()

    invalid_price = df.filter(
        col("unit_price") < 0
    ).count()

    invalid_revenue = df.filter(
        col("revenue").isNull()
    ).count()

    invalid_transaction_type = df.filter(
        col("transaction_type").isNull()
    ).count()

    duplicate_rows = (
        df.groupBy(
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country"
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    return {
        "total_rows": total_rows,
        "null_required_fields": null_required,
        "invalid_price": invalid_price,
        "invalid_revenue": invalid_revenue,
        "invalid_transaction_type": invalid_transaction_type,
        "duplicate_groups": duplicate_rows
    }