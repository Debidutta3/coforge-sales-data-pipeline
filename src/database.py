import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = PROJECT_ROOT / "data" / "sales.db"


def load_sales_to_database(
    processed_file: Path,
    database_file: Path = DATABASE_PATH,
) -> int:
    """Load processed sales into SQLite and return the row count."""
    if not processed_file.is_file():
        raise FileNotFoundError(
            f"Processed sales file not found: {processed_file}"
        )

    sales_df = pd.read_csv(processed_file)

    required_columns = {
        "order_id",
        "order_date",
        "product",
        "quantity",
        "unit_price",
        "customer_id",
        "total_amount",
    }

    missing = required_columns - set(sales_df.columns)
    if missing:
        raise ValueError(f"Missing database columns: {sorted(missing)}")

    database_file.parent.mkdir(parents=True, exist_ok=True)

    #set up a connection to the SQLite database and load the data into a table named "sales"
    with sqlite3.connect(database_file) as connection:
        sales_df.to_sql(
            "sales",
            connection,
            if_exists="replace",
            index=False,
        )

        count = connection.execute(
            "SELECT COUNT(*) FROM sales"
        ).fetchone()[0]

    return count