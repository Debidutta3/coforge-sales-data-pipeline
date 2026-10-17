
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.database import load_sales_to_database


def test_database_loads_all_processed_records(tmp_path):
    processed_file = tmp_path / "processed_sales.csv"
    database_file = tmp_path / "sales.db"

    sales = pd.DataFrame({
        "order_id": [4001, 4002],
        "order_date": ["2026-10-10", "2026-10-11"],
        "product": ["Laptop", "Mouse"],
        "quantity": [1, 2],
        "unit_price": [50000.0, 500.0],
        "customer_id": [601, 602],
        "total_amount": [50000.0, 1000.0],
    })
    sales.to_csv(processed_file, index=False)

    count = load_sales_to_database(processed_file, database_file)

    assert count == 2

    with sqlite3.connect(database_file) as connection:
        rows = connection.execute(
            "SELECT COUNT(*) FROM sales"
        ).fetchone()[0]

    assert rows == 2


def test_database_rejects_missing_processed_file(tmp_path):
    processed_file = tmp_path / "missing.csv"
    database_file = tmp_path / "sales.db"

    with pytest.raises(FileNotFoundError, match="Processed sales file not found"):
        load_sales_to_database(processed_file, database_file)


def test_database_rejects_missing_required_columns(tmp_path):
    processed_file = tmp_path / "incomplete.csv"
    database_file = tmp_path / "sales.db"

    pd.DataFrame({
        "order_id": [4001],
        "product": ["Laptop"],
    }).to_csv(processed_file, index=False)

    with pytest.raises(ValueError, match="Missing database columns"):
        load_sales_to_database(processed_file, database_file)


def test_database_replaces_existing_sales_table(tmp_path):
    processed_file = tmp_path / "processed_sales.csv"
    database_file = tmp_path / "sales.db"

    original_sales = pd.DataFrame({
        "order_id": [4001],
        "order_date": ["2026-10-10"],
        "product": ["Laptop"],
        "quantity": [1],
        "unit_price": [50000.0],
        "customer_id": [601],
        "total_amount": [50000.0],
    })
    original_sales.to_csv(processed_file, index=False)

    load_sales_to_database(processed_file, database_file)

    updated_sales = original_sales.copy()
    updated_sales["order_id"] = 4002
    updated_sales["product"] = "Mouse"
    updated_sales["total_amount"] = 500.0
    updated_sales.to_csv(processed_file, index=False)

    count = load_sales_to_database(processed_file, database_file)

    assert count == 1

    with sqlite3.connect(database_file) as connection:
        rows = connection.execute(
            "SELECT order_id, product FROM sales"
        ).fetchall()

    assert rows == [(4002, "Mouse")]
