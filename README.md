# Coforge Sales Data Engineering Pipeline

## Project Overview

This project implements a sales data processing pipeline that ingests raw sales records, validates data quality, transforms valid records, separates invalid records, and loads the processed data into a SQLite database.

The project also includes a PySpark transformation pipeline and uses Apache Airflow to orchestrate and verify the Pandas ETL workflow.

## Problem Statement

Sales datasets can contain missing values, invalid dates, incorrect numeric values, duplicate order IDs, and invalid quantities or prices. Processing such data without validation can lead to unreliable reporting.

This project addresses these challenges by validating incoming sales data, isolating rejected records with reasons, calculating sales amounts, and storing valid records in a database.

## Architecture

1. **Data ingestion:** Read raw sales records from CSV.
2. **Data validation:** Check required fields, numeric values, dates, duplicate order IDs, quantities, and prices.
3. **Data transformation:** Calculate the total amount for valid sales records.
4. **Data quality handling:** Separate rejected records and record rejection reasons.
5. **Database loading:** Load valid records into SQLite.
6. **Workflow orchestration:** Use Apache Airflow to validate the input file, execute the Pandas ETL pipeline, and verify the output files and database.
7. **Spark processing:** Run a separate PySpark transformation pipeline locally.

## Technology Stack

- Python
- Pandas
- PySpark
- SQLite
- Apache Airflow
- Docker and Docker Compose
- pytest
- Git

## Project Structure

```text
coforge-capstone/
├── dags/
│   └── sales_etl_dag.py
├── data/
│   ├── raw_sales.csv
│   ├── test_sales.csv
│   ├── processed_sales.csv
│   ├── rejected_sales.csv
│   ├── spark_processed_sales.csv
│   └── sales.db
├── logs/
├── src/
│   ├── main.py
│   ├── database.py
│   └── spark_pipeline.py
├── tests/
│   ├── test_pipeline.py
│   └── test_spark_pipeline.py
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

Generated output files and the SQLite database are excluded from Git.

## How to Run

### 1. Activate the Python environment

On Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

### 3. Run the Pandas ETL pipeline

```powershell
python -m src.main
```

### 4. Run the PySpark pipeline

```powershell
python -m src.spark_pipeline
```

The Spark pipeline runs locally and produces `data/spark_processed_sales.csv`.

### 5. Run tests

```powershell
python -m pytest -v
```

### 6. Start Airflow

With Docker Desktop running:

```powershell
docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080), locate `coforge_sales_etl`, and trigger a DAG run.

The DAG contains three tasks:

1. `validate_input` — checks that the input CSV exists and contains the required columns.
2. `run_pandas_etl` — executes the Pandas ETL pipeline.
3. `verify_outputs` — verifies the output files and checks the SQLite record count and revenue.

To stop the Airflow container:

```powershell
docker compose down
```

## Validation and Test Results

The project has been tested with the following results:

- Main sales dataset: 10 records processed successfully.
- SQLite database: 10 records loaded.
- Total sales revenue: 180850.0.
- Test dataset: 7 records, with 2 valid records and 5 rejected records.
- PySpark output: 10 records with total revenue of 180850.0.
- Airflow workflow: all three tasks completed successfully in a verified run.
- Unit tests: four Pandas pipeline tests and three Spark pipeline tests passed in the tested runs.

## Current Scope and Limitations

- SQLite is used as a lightweight local database.
- PySpark runs separately in local mode; it is not currently executed by the Airflow DAG.
- The Airflow workflow orchestrates the Pandas ETL pipeline.
- The current dataset is small and intended for demonstrating ETL, validation, testing, and orchestration concepts.

## Future Improvements

- Integrate Spark execution into Airflow.
- Add scheduled or file-arrival-based processing.
- Add more data-quality checks and automated failure alerts.
- Add cloud storage and a managed database.
- Extend the pipeline with larger datasets and performance testing.

## Learning Outcomes

This project demonstrates practical experience with ETL design, data validation, error handling, SQL database loading, PySpark transformations, unit testing, Docker-based deployment, Airflow orchestration, and Git version control.


