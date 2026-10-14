import pytest
from pathlib import Path
from pyspark.sql import SparkSession

from src.spark_pipeline import process_sales_data


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("CoforgeSparkTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_spark_processes_all_sales_records(spark):
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / "data" / "raw_sales.csv"

    result = process_sales_data(spark, input_path)

    assert result.count() == 10


def test_spark_calculates_correct_revenue(spark):
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / "data" / "raw_sales.csv"

    result = process_sales_data(spark, input_path)

    revenue = result.agg(
        {"total_amount": "sum"}
    ).first()[0]

    assert revenue == pytest.approx(180850.0)


def test_spark_rejects_missing_file(spark, tmp_path):
    missing_path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        process_sales_data(spark, missing_path)