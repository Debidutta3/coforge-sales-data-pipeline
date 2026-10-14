
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="coforge_sales_etl",
    description="Validate input, run sales ETL, and verify outputs",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["coforge", "sales", "etl"],
) as dag:

    validate_input = BashOperator(
        task_id="validate_input",
        bash_command=(
            "python -c \""
            "import csv, os; "
            "p='/opt/airflow/data/raw_sales.csv'; "
            "required={'order_id','order_date','product','quantity','unit_price','customer_id'}; "
            "assert os.path.isfile(p), f'Missing input file: {p}'; "
            "f=open(p, newline='', encoding='utf-8'); "
            "reader=csv.DictReader(f); "
            "missing=required-set(reader.fieldnames or []); "
            "f.close(); "
            "assert not missing, f'Missing columns: {sorted(missing)}'; "
            "print('Input validation passed: file exists and required columns are present')"
            "\""
        ),
    )

    run_pandas_etl = BashOperator(
        task_id="run_pandas_etl",
        bash_command="cd /opt/airflow && python -m src.main",
    )

    verify_outputs = BashOperator(
        task_id="verify_outputs",
        bash_command=(
            "python -c \""
            "import csv, os, sqlite3; "
            "base='/opt/airflow/data'; "
            "files=['processed_sales.csv','rejected_sales.csv','sales.db']; "
            "missing=[name for name in files if not os.path.isfile(os.path.join(base,name))]; "
            "assert not missing, f'Missing outputs: {missing}'; "
            "c=sqlite3.connect(os.path.join(base,'sales.db')); "
            "count=c.execute('SELECT COUNT(*) FROM sales').fetchone()[0]; "
            "revenue=c.execute('SELECT SUM(total_amount) FROM sales').fetchone()[0]; "
            "c.close(); "
            "print(f'Output verification passed: {count} database rows, total revenue={revenue}')"
            "\""
        ),
    )

    validate_input >> run_pandas_etl >> verify_outputs
