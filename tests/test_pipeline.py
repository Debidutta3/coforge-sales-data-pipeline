from pathlib import Path

import pandas as pd

from src.main import ingest_sales_data, validate_and_transform


def test_ingestion_loads_all_sales_records():
    file_path = Path("data/raw_sales.csv")

    df = ingest_sales_data(file_path)

    assert len(df) == 10
    assert "order_id" in df.columns
    assert "customer_id" in df.columns


def test_validation_separates_valid_and_invalid_records():
    file_path = Path("data/test_sales.csv")

    df = ingest_sales_data(file_path)
    valid, rejected = validate_and_transform(df)

    assert len(df) == len(valid) + len(rejected)
    assert len(valid) == 2
    assert len(rejected) == 5


def test_valid_records_have_correct_sales_totals():
    file_path = Path("data/test_sales.csv")

    df = ingest_sales_data(file_path)
    valid, _ = validate_and_transform(df)

    laptop = valid.loc[valid["order_id"] == 2001].iloc[0]

    assert laptop["total_amount"] == 1000.0


def test_rejected_records_include_reasons():
    file_path = Path("data/test_sales.csv")

    df = ingest_sales_data(file_path)
    _, rejected = validate_and_transform(df)

    assert "rejection_reason" in rejected.columns
    assert rejected["rejection_reason"].notna().all()
    assert rejected["rejection_reason"].str.len().gt(0).all()