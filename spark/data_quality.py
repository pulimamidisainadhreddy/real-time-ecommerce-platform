from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    isnan,
    try_to_timestamp,
    sum as spark_sum,
)


# ============================================================
# Configuration
# ============================================================

REQUIRED_COLUMNS = [
    "invoice_no",
    "stock_code",
    "quantity",
    "unit_price",
    "invoice_date",
]

VALID_TRANSACTION_TYPES = [
    "SALE",
    "RETURN",
    "CANCELLED",
]

DUPLICATE_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
]


# ============================================================
# Data-quality checks
# ============================================================

def check_data_quality(df: DataFrame) -> dict:
    """
    Run data-quality checks on transformed Spark data.

    Checks:
    - Missing required values
    - Invalid prices
    - Invalid quantities
    - Invalid dates
    - Duplicate records
    - Negative revenue
    - Invalid transaction types
    """

    quality_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    # ========================================================
    # Required-column check
    # ========================================================

    missing_columns = [
        column_name
        for column_name in REQUIRED_COLUMNS
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )

    df = df.cache()

    try:
        total_rows = df.count()

        # ====================================================
        # Missing-value check
        # ====================================================

        null_condition = None

        for column_name in REQUIRED_COLUMNS:
            condition = col(column_name).isNull()

            if null_condition is None:
                null_condition = condition
            else:
                null_condition = (
                    null_condition | condition
                )

        null_required_fields = df.filter(
            null_condition
        ).count()

        # ====================================================
        # Invalid-price check
        # ====================================================

        invalid_price = df.filter(
            col("unit_price").isNull()
            | isnan(col("unit_price"))
            | (col("unit_price") < 0)
        ).count()

        # ====================================================
        # Invalid-quantity check
        # ====================================================

        invalid_quantity = df.filter(
            col("quantity").isNull()
            | (col("quantity") == 0)
        ).count()

        # ====================================================
        # Invalid-date check
        # ====================================================

        invalid_date = df.filter(
            col("invoice_date").isNull()
            | try_to_timestamp(
                col("invoice_date")
            ).isNull()
        ).count()

        # ====================================================
        # Revenue checks
        # ====================================================

        if "revenue" in df.columns:
            invalid_revenue = df.filter(
                col("revenue").isNull()
                | isnan(col("revenue"))
            ).count()

            negative_revenue = df.filter(
                col("revenue") < 0
            ).count()
        else:
            invalid_revenue = total_rows
            negative_revenue = 0

        # ====================================================
        # Transaction-type check
        # ====================================================

        if "transaction_type" in df.columns:
            invalid_transaction_type = df.filter(
                col("transaction_type").isNull()
                | ~col("transaction_type").isin(
                    *VALID_TRANSACTION_TYPES
                )
            ).count()
        else:
            invalid_transaction_type = total_rows

        # ====================================================
        # Duplicate-record check
        # ====================================================

        available_duplicate_columns = [
            column_name
            for column_name in DUPLICATE_COLUMNS
            if column_name in df.columns
        ]

        duplicate_groups = 0
        duplicate_rows = 0

        if available_duplicate_columns:
            duplicate_groups_df = (
                df.groupBy(
                    *available_duplicate_columns
                )
                .count()
                .filter(col("count") > 1)
            )

            duplicate_groups = (
                duplicate_groups_df.count()
            )

            duplicate_rows_result = (
                duplicate_groups_df
                .select(
                    (
                        col("count") - 1
                    ).alias("duplicate_rows")
                )
                .agg(
                    spark_sum(
                        "duplicate_rows"
                    ).alias(
                        "total_duplicate_rows"
                    )
                )
                .collect()[0][
                    "total_duplicate_rows"
                ]
            )

            duplicate_rows = int(
                duplicate_rows_result or 0
            )

        # ====================================================
        # Overall quality result
        # ====================================================

        # Negative revenue is valid for RETURN transactions.
        # Duplicate rows are handled later by transform_data().
        quality_check_passed = (
            null_required_fields == 0
            and invalid_price == 0
            and invalid_quantity == 0
            and invalid_date == 0
            and invalid_revenue == 0
            and invalid_transaction_type == 0
        )

        quality_results = {
            "quality_timestamp": quality_timestamp,
            "total_rows": total_rows,
            "null_required_fields": (
                null_required_fields
            ),
            "invalid_price": invalid_price,
            "invalid_quantity": invalid_quantity,
            "invalid_date": invalid_date,
            "invalid_revenue": invalid_revenue,
            "negative_revenue": negative_revenue,
            "invalid_transaction_type": (
                invalid_transaction_type
            ),
            "duplicate_groups": duplicate_groups,
            "duplicate_rows": duplicate_rows,
            "duplicate_check_passed": (
                duplicate_rows == 0
            ),
            "quality_check_passed": (
                quality_check_passed
            ),
        }
        print("DATA QUALITY RESULTS:", quality_results)
        return quality_results

    finally:
        df.unpersist()