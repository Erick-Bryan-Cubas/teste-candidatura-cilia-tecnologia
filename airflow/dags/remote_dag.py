from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'remote_dag',
    default_args=default_args,
    description='Remote data extraction and loading DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/scripts/extract.py',
    )

    load_csv_to_postgres = BashOperator(
        task_id='load_csv_to_postgres',
        bash_command='python /opt/airflow/scripts/load_csv_to_postgres_airflow.py',
    )

    extract_task >> load_csv_to_postgres
