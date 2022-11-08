from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG



default_args = {
    'owner': 'mykola',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}


@dag(default_args=default_args,
    dag_id= 'dag_with_taskflow-V02',
    description='learning taskflow',
    start_date=datetime(2022, 10, 5),
    schedule_interval='@daily')


def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Jerry',
            'last_name' : 'Heil'
        }
    
    @task
    def get_age():
        return 19

    @task
    def greet(first_name, last_name, age):
         print(f'Hello World! My name is {first_name} {last_name} and i am {age} years old!')

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], 
        last_name=name_dict['last_name'], 
        age=age)

greet_dag = hello_world_etl()