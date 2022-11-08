from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'mykola',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id= 'dag_with_catchup_and_backfill_v2',
    description='learning taskflow',
    start_date=datetime(2022, 10, 6),
    schedule_interval='@daily',
    catchup=False) as dag:

    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo Tis is simple bash command'
    )

task1