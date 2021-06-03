from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from plugins.etl import process_api_hdfs, get_table_names, process_table_hdfs, api_data_to_silver, db_table_to_silver, process_to_gold

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

##########################################################

with DAG(
    dag_id='etl_all',
    description='Extract data from API and DB to bronze, transfer to silver and save to gold',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['etl', 'all', 'bronze', 'silver', 'gold'],

) as etl_dag:

    api_to_bronze = PythonOperator(
        task_id='api_to_bronze',
        python_callable=process_api_hdfs,
        )

    api_to_silver = PythonOperator(
        task_id='api_to_silver',
        python_callable=api_data_to_silver,
        )

    to_bronze = []
    for table_name in get_table_names():
        sub_task = PythonOperator(
                        task_id=f'db_table_{table_name}_to_bronze',
                        python_callable=process_table_hdfs,
                        op_kwargs={'table_name': table_name},
                        )
        to_bronze.append(sub_task)

    to_bronze.append(api_to_bronze)

    dummy_bronze = DummyOperator(
                        task_id="dummy_bronze",
                        )

    to_silver = []
    for table_name in get_table_names():
        sub_task = PythonOperator(
                        task_id=f'db_table_{table_name}_to_silver',
                        python_callable=db_table_to_silver,
                        op_kwargs={'table_name': table_name},
                        )
        to_silver.append(sub_task)

    to_silver.append(api_to_silver)

    dummy_silver = DummyOperator(
                        task_id="dummy_silver",
                        )

    load_to_gold = PythonOperator(
                        task_id='load_to_gold',
                        python_callable=process_to_gold,
                        )

    to_bronze >> dummy_bronze >> to_silver >> dummy_silver >> load_to_gold
