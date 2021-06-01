from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from plugins.etl import process_api_hdfs, api_to_silver

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

##########################################################

with DAG(
    dag_id='api_data_etl',
    description='Save data from API to bronze, then transfer to silver',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['bronze', 'api'],

) as api_dag:

    t1 = PythonOperator(
        task_id='process_api_to_bronze',
        python_callable=process_api_hdfs,
        )

    t2 = PythonOperator(
        task_id='process_api_to_silver',
        python_callable=api_to_silver,
        )

    t1 >> t2