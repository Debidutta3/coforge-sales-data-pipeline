from pathlib import Path
import csv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw_sales.csv"
SPARK_OUTPUT_PATH = PROJECT_ROOT / "data" / "spark_processed_sales.csv"


def create_spark_session() -> SparkSession:
    """Create a local Spark session for sales processing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("CoforgeSalesPipeline")
        .getOrCreate()
    )


def process_sales_data(spark: SparkSession, input_path: Path):
    """Read sales data, validate required columns, and calculate order totals."""

    if not input_path.is_file():
        raise FileNotFoundError(f"Sales file not found: {input_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )

    required_columns = {
        "order_id",
        "order_date",
        "product",
        "quantity",
        "unit_price",
        "customer_id",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    result = df.withColumn(
        "total_amount",
        col("quantity") * col("unit_price"),
    )

    return result


def write_results(result, output_path: Path) -> None:
    """Write Spark result rows to a local CSV without Hadoop's CSV writer."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open(
        "w", newline="", encoding="utf-8"
    ) as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=result.columns,
        )

        writer.writeheader()

        for row in result.toLocalIterator():
            writer.writerow(row.asDict())


def main() -> None:
    spark = create_spark_session()

    try:
        spark.sparkContext.setLogLevel("ERROR")

        print("Starting Spark sales pipeline...")

        result = process_sales_data(spark, RAW_DATA_PATH)

        record_count = result.count()

        total_revenue = result.agg(
            {"total_amount": "sum"}
        ).first()[0]

        write_results(result, SPARK_OUTPUT_PATH)

        print(f"Records processed: {record_count}")
        print(f"Total revenue: {total_revenue}")
        print(f"Output file: {SPARK_OUTPUT_PATH}")

    except Exception:
        print("Spark sales pipeline failed.")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()