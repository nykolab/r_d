from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from homework import dump_api_to_disk, dump_db_to_disk, dump_api_to_hdfs, dump_db_to_hdfs

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='homework_7',
    description='Dag for homework 7',
    schedule_interval='@hourly',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['homework'],

) as dag_disk:
    
    t1 = PythonOperator(
    task_id='api_dump_with_airflow',
    python_callable=dump_api_to_disk,
    )

    t2 = PythonOperator(
    task_id='db_dump_with_airflow',
    python_callable=dump_db_to_disk,
    )
    
    t1 >> t2


with DAG(
    dag_id='homework_hdfs',
    description='Dag for homework hdfs',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['homework'],

) as dag_hdfs:
    
    t1 = PythonOperator(
    task_id='api_dump_to_hdfs',
    python_callable=dump_api_to_hdfs,
    )

    t2 = PythonOperator(
    task_id='db_dump_to_hdfs',
    python_callable=dump_db_to_hdfs,
    )

    t1 >> t2