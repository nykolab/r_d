from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from plugins.etl import get_table_names, db_table_to_silver, api_to_silver

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

##########################################################

with DAG(
    dag_id='etl_transfer_to_silver',
    description='Validate data from bronze and transfer to silver',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,21,0),
    tags=['bronze', 'transfer', 'silver'],

) as dag_transform_to_silver:

    def to_silver_group(table_name):
        return PythonOperator(
            task_id=f'{table_name}_to_silver',
            python_callable=db_table_to_silver,
            op_kwargs={'table_name': table_name},
            )

    end = DummyOperator(
        task_id="end_dummy",
        )

    api_to_silver = PythonOperator(
        task_id='api_to_silver',
        python_callable=api_to_silver,
        )

    api_to_silver >> end

    for table_name in get_table_names():
        to_silver_group(table_name) >> end



