from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import pytest

from spark.transformations import classify_transaction, transform_data


def test_transaction_classification():
    assert classify_transaction("C100", 1) == "CANCELLED"
    assert classify_transaction("100", -2) == "RETURN"
    assert classify_transaction("100", 2) == "SALE"


def test_transaction_classification_with_none_quantity():
    assert classify_transaction("100", None) == "SALE"


@pytest.fixture
def retail_schema():
    return StructType([
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("invoice_date", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
    ])


def make_df(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


def base_row(invoice="100", stock="A1", quantity=2, price=10.0):
    return (
        invoice,
        stock,
        "Product",
        quantity,
        "2025-01-01 10:00:00",
        price,
        "C1",
        "Germany",
    )


def test_revenue_calculation(spark, retail_schema):
    df = make_df(spark, retail_schema, [base_row(quantity=3, price=12.50)])

    result = transform_data(df).collect()

    assert len(result) == 1
    assert result[0]["revenue"] == 37.5
    assert result[0]["transaction_type"] == "SALE"


def test_return_revenue_is_negative(spark, retail_schema):
    df = make_df(spark, retail_schema, [base_row(quantity=-2, price=10.0)])

    result = transform_data(df).collect()

    assert result[0]["revenue"] == -20.0
    assert result[0]["transaction_type"] == "RETURN"


def test_duplicate_records_are_removed(spark, retail_schema):
    row = base_row()
    df = make_df(spark, retail_schema, [row, row])

    result = transform_data(df)

    assert result.count() == 1


def test_invalid_zero_quantity_is_removed(spark, retail_schema):
    df = make_df(spark, retail_schema, [base_row(quantity=0)])

    result = transform_data(df)

    assert result.count() == 0


def test_negative_price_is_removed(spark, retail_schema):
    df = make_df(spark, retail_schema, [base_row(price=-5.0)])

    result = transform_data(df)

    assert result.count() == 0


def test_missing_required_column_raises(spark):
    df = spark.createDataFrame([(1,)], ["invoice_no"])

    with pytest.raises(ValueError, match="Missing required columns"):
        transform_data(df)
