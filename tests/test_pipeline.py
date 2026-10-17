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
    
    

def test_ingestion_rejects_missing_file():
    file_path = Path("data/does_not_exist.csv")

    try:
        ingest_sales_data(file_path)
        assert False, "Expected FileNotFoundError was not raised"
    except FileNotFoundError as error:
        assert "Sales file not found" in str(error)


def test_ingestion_rejects_missing_required_columns(tmp_path):
    file_path = tmp_path / "incomplete_sales.csv"

    # This CSV intentionally omits customer_id and unit_price.
    pd.DataFrame({
        "order_id": [3001],
        "order_date": ["2026-10-10"],
        "product": ["Laptop"],
        "quantity": [1],
    }).to_csv(file_path, index=False)

    try:
        ingest_sales_data(file_path)
        assert False, "Expected ValueError was not raised"
    except ValueError as error:
        assert "Missing required columns" in str(error)
