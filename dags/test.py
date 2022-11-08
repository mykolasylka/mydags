
from email.policy import default
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'mykola',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='second_name', value='Heil')

def get_age(ti):
    ti.xcom_push(key='age', value=25)

def greet(ti):
    first_name = ti.xcom_pull(task_ids = 'get_name', key = 'first_name')
    second_name = ti.xcom_pull(task_ids = 'get_name', key = 'second_name')
    age = ti.xcom_pull(task_ids = 'get_age', key = 'age')
    print(f'Hello World! My name is {first_name} {second_name}, and i am {age} years old!')



with DAG(
    default_args=default_args,
    dag_id= 'Xcoms-V01',
    description='learning xcoms',
    start_date=datetime(2022, 10, 5),
    schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet
    )


    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
        
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
        
    )


    [task2, task3] >> task1