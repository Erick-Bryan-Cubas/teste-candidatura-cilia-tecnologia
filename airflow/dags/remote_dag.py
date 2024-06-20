from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def query_covid_data():
    try:
        conn = psycopg2.connect(
            host='postgres',
            dbname='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM covid_data LIMIT 5;")
        rows = cur.fetchall()
        for row in rows:
            logging.info(row)
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error querying data from PostgreSQL: {e}")

with DAG(
    'remote_dag',
    default_args=default_args,
    description='Remote data extraction and loading DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    query_task = PythonOperator(
        task_id='query_covid_data',
        python_callable=query_covid_data,
        execution_timeout=timedelta(minutes=5)
    )

    query_task
