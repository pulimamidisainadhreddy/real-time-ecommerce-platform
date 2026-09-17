import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark.data_quality import check_data_quality


@pytest.fixture
def data_schema():
    return StructType([
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("invoice_date", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("revenue", DoubleType(), True),
    ])


def valid_row():
    return (
        "100", "A1", "Product", 2, "2025-01-01 10:00:00",
        10.0, "C1", "Germany", "SALE", 20.0,
    )


def create_dataframe(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


def test_valid_data_passes(spark, data_schema):
    result = check_data_quality(create_dataframe(spark, data_schema, [valid_row()]))
    assert result["quality_check_passed"] is True


def test_missing_values_fail(spark, data_schema):
    row = list(valid_row())
    row[0] = None
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["null_required_fields"] == 1
    assert result["quality_check_passed"] is False


def test_invalid_price_fails(spark, data_schema):
    row = list(valid_row())
    row[5] = -10.0
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["invalid_price"] == 1
    assert result["quality_check_passed"] is False


def test_invalid_quantity_fails(spark, data_schema):
    row = list(valid_row())
    row[3] = 0
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["invalid_quantity"] == 1
    assert result["quality_check_passed"] is False


def test_invalid_date_fails(spark, data_schema):
    row = list(valid_row())
    row[4] = "invalid-date"
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["invalid_date"] == 1
    assert result["quality_check_passed"] is False


def test_duplicate_records_are_detected(spark, data_schema):
    row = valid_row()
    result = check_data_quality(create_dataframe(spark, data_schema, [row, row]))
    assert result["duplicate_groups"] == 1
    assert result["duplicate_rows"] == 1
    assert result["duplicate_check_passed"] is False


def test_negative_revenue_is_detected(spark, data_schema):
    row = list(valid_row())
    row[9] = -20.0
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["negative_revenue"] == 1
    assert result["quality_check_passed"] is True


def test_invalid_transaction_type_fails(spark, data_schema):
    row = list(valid_row())
    row[8] = "INVALID_TYPE"
    result = check_data_quality(create_dataframe(spark, data_schema, [tuple(row)]))
    assert result["invalid_transaction_type"] == 1
    assert result["quality_check_passed"] is False
