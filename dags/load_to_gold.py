from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from plugins.etl import process_api_to_gold

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

##########################################################

with DAG(
    dag_id='etl_load_to_silver',
    description='Load data to gold',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,22,0),
    tags=['gold', 'load'],

) as dag_load_to_gold:

    load_to_gold = PythonOperator(
        task_id='load_to_gold',
        python_callable=process_api_to_gold,
        )

    load_to_gold