import os
from datetime import datetime, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_pipeline():
    print("Online Retail pipeline check started.")
    print("Kafka → Spark → PostgreSQL → Power BI")


def check_database():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "host.docker.internal"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        database=os.getenv("POSTGRES_DB", "online_retail"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root"),
    )

    cursor = connection.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM public.online_retail;"
    )

    row_count = cursor.fetchone()[0]

    print(f"PostgreSQL row count: {row_count}")

    if row_count < 500000:
        raise ValueError(
            f"Unexpectedly low row count: {row_count}"
        )

    cursor.close()
    connection.close()

    print("PostgreSQL database check passed.")


def check_transaction_data():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "host.docker.internal"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        database=os.getenv("POSTGRES_DB", "online_retail"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root"),
    )

    cursor = connection.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM public.online_retail
        WHERE transaction_type IN ('SALE', 'RETURN');
    """)

    transaction_count = cursor.fetchone()[0]

    print(f"Valid transaction records: {transaction_count}")

    if transaction_count == 0:
        raise ValueError(
            "No valid transaction records found."
        )

    cursor.close()
    connection.close()

    print("Transaction data check passed.")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="online_retail_pipeline",
    default_args=default_args,
    description="Orchestrate the Online Retail data engineering pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    pipeline_check = PythonOperator(
        task_id="pipeline_check",
        python_callable=check_pipeline,
    )

    database_check = PythonOperator(
        task_id="database_check",
        python_callable=check_database,
    )

    transaction_check = PythonOperator(
        task_id="transaction_check",
        python_callable=check_transaction_data,
    )

    pipeline_check >> database_check >> transaction_check