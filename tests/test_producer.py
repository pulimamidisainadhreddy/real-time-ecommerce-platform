import json
from types import SimpleNamespace

import pandas as pd

from producer.producer import (
    create_event,
    create_message_key,
    validate_row,
)


def test_sample_dataset_exists():
    df = pd.read_csv("data/sample/ecommerce_sample.csv")

    assert len(df) > 0
    assert "InvoiceNo" in df.columns
    assert "StockCode" in df.columns
    assert "Quantity" in df.columns
    assert "UnitPrice" in df.columns


def valid_row():
    return SimpleNamespace(
        InvoiceNo="10001",
        StockCode="A1",
        Description="Product",
        Quantity=2,
        InvoiceDate="2025-01-01 10:00:00",
        UnitPrice=10.50,
        CustomerID="C1",
        Country="Germany",
    )


def test_create_event_contains_expected_values():
    event = create_event(valid_row())

    assert event["event_version"] == "1.0"
    assert event["invoice_no"] == "10001"
    assert event["quantity"] == 2
    assert event["unit_price"] == 10.50


def test_json_conversion_round_trip():
    event = create_event(valid_row())

    encoded = json.dumps(event)
    decoded = json.loads(encoded)

    assert decoded == event


def test_create_message_key_uses_invoice_number():
    event = create_event(valid_row())

    assert create_message_key(event) == "10001"


def test_create_message_key_falls_back_to_stock_code():
    event = create_event(valid_row())
    event["invoice_no"] = None

    assert create_message_key(event) == "A1"


def test_valid_record_passes_validation():
    valid, reason = validate_row(valid_row())

    assert valid is True
    assert reason == ""


def test_invalid_record_with_negative_price_fails():
    row = valid_row()
    row.UnitPrice = -1.0

    valid, reason = validate_row(row)

    assert valid is False
    assert "negative" in reason.lower()


def test_invalid_record_with_missing_invoice_fails():
    row = valid_row()
    row.InvoiceNo = None

    valid, reason = validate_row(row)

    assert valid is False
    assert "InvoiceNo" in reason


def test_invalid_record_with_non_numeric_quantity_fails():
    row = valid_row()
    row.Quantity = "two"

    valid, reason = validate_row(row)

    assert valid is False
    assert "numeric" in reason.lower()
