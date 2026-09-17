import pandas as pd


def test_sample_dataset_exists():
    df = pd.read_csv("data/sample/ecommerce_sample.csv")

    assert len(df) > 0
    assert "InvoiceNo" in df.columns
    assert "StockCode" in df.columns
    assert "Quantity" in df.columns
    assert "UnitPrice" in df.columns