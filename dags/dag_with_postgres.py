from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.connection import Connection
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

default_args = {
    'owner': 'mykola',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}


with DAG(
    default_args=default_args,
    dag_id='DAG_Postgres_operator_v7',
    description='Firts dag using postgres operator',
    start_date=datetime(2022, 10, 9, 2),
    schedule_interval='@daily'

) as dag:


    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id ='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )


    task2 = PostgresOperator(
        task_id='insert_postgres_table',
        postgres_conn_id ='postgres_localhost',
        sql="""
            insert into dag_runs(dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id}}')
            
        """
    )


    task3 = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id ='postgres_localhost',
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id}}';
            
        """
    )
    task1 >> task3 >> task2


