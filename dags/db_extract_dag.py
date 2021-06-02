from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from plugins.etl import get_table_names, process_table_hdfs

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

##########################################################

with DAG(
    dag_id='etl_db_extract',
    description='Save data from DB to bronze',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['bronze', 'db'],

) as dag_hdfs_bronze:

    def to_bronze_group(table_name):
        return PythonOperator(
            task_id=f'{table_name}_dump_to_hdfs',
            python_callable=process_table_hdfs,
            op_kwargs={'table_name': table_name},
            )

    dummy = DummyOperator(
        task_id="dummy_bronze",
        )

    for table_name in get_table_names():
        to_bronze_group(table_name) >> dummy
    