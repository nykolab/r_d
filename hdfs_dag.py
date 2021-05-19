from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from homework import dump_api_to_hdfs, dump_db_to_hdfs

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

with DAG(
    dag_id='homework_hdfs',
    description='Dag for hdfs,
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['homework', 'hdfs'],

) as dag:
    
    t1 = PythonOperator(
    task_id='api_dump_with_airflow_to_hdfs',
    python_callable=dump_api_to_hdfs,
    )

    t2 = PythonOperator(
    task_id='db_dump_with_airflow_to_hdfs',
    python_callable=dump_db_to_hdfs,
    )
    
    t1 >> t2