from genericpath import exists
import pathlib
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import pathlib
import os
import requests
import psycopg2

default_args = {
    'owner': 'mykola',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}


def get_data(**kwargs):
    # if not os.path.exists('./dags/data'):
    #     os.makedirs('./dags/data')
    # print([x[0] for x in os.walk('/opt/***/dags')])
    # print(os.getcwd())
    response = requests.get("https://jsonplaceholder.typicode.com/comments")
    df = pd.read_json(response.text)
    # print(df.head())
    # logging.info('loading data from API')
    path = '/opt/airflow/data/get_data.csv'
    df.to_csv(path)
    kwargs['ti'].xcom_push(key='test', value=path)

def transform_df(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='test', task_ids = 'get_data')
    print('DATA DATA', data)
    df = pd.read_csv(data).head(10)
    df.sort_values(by=['name'], ascending = False, inplace = True)
    path = '/opt/airflow/data/final2.csv'
    df.to_csv(path)
    kwargs['ti'].xcom_push(key='test2', value=path)


def connect(database, user, password, host, port):
    connection = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port)
    return connection


def save_to_db(**kwargs):
    
    print(os.walk('.'))
    conn = connect('test', 'airflow', 'airflow', 'postgres', 5432)
    ti = kwargs['ti']
    data = ti.xcom_pull(key='test2', task_ids = 'transform_df')
    df = pd.read_csv(data)
    print(df)
    df.to_sql('data', con=conn, if_exists='replace')
    


with DAG(
    default_args=default_args,
    dag_id='dag_with_python_oerator_v022',
    description='Firts dag using python operator',
    start_date=datetime(2022, 10, 5, 2),
    schedule_interval='@daily'

) as dag:


    task1 = PythonOperator(
        task_id='get_data',
        python_callable=get_data
        
    
    )

    task2 = PythonOperator(
        task_id='transform_df',
        python_callable=transform_df
    
    )


    task3 = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_db
    
    )

    task1 >> task2 >> task3