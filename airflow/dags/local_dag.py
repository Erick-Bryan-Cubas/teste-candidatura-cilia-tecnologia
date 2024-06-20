from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'local_dag',
    default_args=default_args,
    description='Local data transformation and reporting DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id='start')

    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/scripts/extract.py',
    )

    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='python /opt/airflow/scripts/transform.py',
    )

    send_task = BashOperator(
        task_id='send_report',
        bash_command='python /opt/airflow/scripts/send.py',
    )

    start_task >> extract_task >> transform_task >> send_task
