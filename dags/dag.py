from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from homework import get_table_names, process_table_hdfs, process_table_disk

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='homework',
    description='Save data from DB and API to HDFS and disk',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2021,5,8,20,0),
    tags=['homework'],

) as dag_hdfs:

    # Dump DB to HDFS
    for table_name in get_table_names():
        t1 = PythonOperator(
        task_id=f'{table_name}_dump_to_hdfs',
        python_callable=process_table_hdfs,
        op_kwargs={'table_name': table_name},
        )

    # Dump DB to disk
    for table_name in get_table_names():
        t1 = PythonOperator(
        task_id=f'{table_name}_dump_to_disk',
        python_callable=process_table_disk,
        op_kwargs={'table_name': table_name},
        )

    # Dump API to HDFS
    t1 = PythonOperator(
    task_id=f'{table_name}_dump_to_hdfs',
    python_callable=process_table_hdfs,
    op_kwargs={'table_name': table_name},
    )

    # Dump API to disk
    t1 = PythonOperator(
    task_id=f'{table_name}_dump_to_hdfs',
    python_callable=process_table_hdfs,
    op_kwargs={'table_name': table_name},
    )