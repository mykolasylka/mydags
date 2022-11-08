from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Mykola Sylka',
    'start_date': days_ago(0),
    'email': ['mykolasylka@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvf /home/project/airflow/tolldata.tgz -C /home/project/airflow/',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command ='cut -d"," -f 1-4 /home/project/airflow/vehicle-data.csv > /home/project/airflow/csv_data.csv',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command ='cut -f 5-7 /home/project/airflow/tollplaza-data.tsv > /home/project/airflow/tsv_data.csv --output-delimiter=","',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command='cut -c 59-61,63-68 /home/project/airflow/payment-data.txt > /home/project/airflow/fixed_width_data.csv --output-delimiter=","',
    dag=dag
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command='paste /home/project/airflow/csv_data.csv /home/project/airflow/tsv_data.csv \
                /home/project/airflow/fixed_width_data.csv > /home/project/airflow/extracted_data.csv',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk \'BEGIN{FS=","; OFS=","} {print $1,$2,$3,toupper($4),$5,$6,$7,$8,$9}\' \
                /home/project/airflow/extracted_data.csv > /home/project/airflow/transformed_data.csv',
    dag=dag,
)



unzip_data>>extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width>>consolidate_data>>transform_data