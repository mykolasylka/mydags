from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import psycopg2

def print_hello():
    return 'Hello world from first Airflow DAG!'

def connect(database, user, password, host, port):
    connection = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port)
    return connection
def get_data():
    conn = connect('test', 'airflow', 'airflow', 'postgres', '5432')
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets")
    for row in cur:
        print(row)



dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 10, 5), catchup=False)

task1 = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
task2 = PythonOperator(task_id='data_task', python_callable=get_data, dag=dag)




task1 >> task2