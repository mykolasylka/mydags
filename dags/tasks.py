from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'mykola',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}



def connect_s3():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1', # глянути в аккауні с3
        aws_access_key_id='AKIAUCAUK76AZL5LEW5V', 
        aws_secret_access_key='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')
    return s3

def download_file():
    s3 = connect_s3()
    # Download file and read from disc
    s3.Bucket('my-first-data').download_file(Key='WHO-COVID-19-global-data.csv', Filename='WHO-COVID-19-global-data.csv')
    df = pd.read_csv('WHO-COVID-19-global-data.csv', index_col=0)
    # return {'path':'WHO-COVID-19-global-data.csv'}
    return df

def write_data():
    df = download_file()
    df = pd.DataFrame(df)
    df.to_sql('')


def connect_psql():
    connection = psycopg2.connect(
        database='test',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432')
    return connection



with DAG(
    default_args=default_args,
    dag_id='S3_test_connect_v04',
    description='Firts dag using python operator',
    start_date=datetime(2022, 9, 28, 2),
    schedule_interval='@daily'

) as dag:


    task1 = PythonOperator(
        task_id='connect_s3',
        python_callable=download_file
        
    )


    task2 = PythonOperator(
        task_id='connect_postgres',
        python_callable=connect_psql
        
    )

    task1 >> task2 
    