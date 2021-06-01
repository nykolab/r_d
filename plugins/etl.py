
import logging

import psycopg2
from contextlib import closing

from datetime import datetime

from hdfs import InsecureClient

from airflow.hooks.base_hook import BaseHook

import json

import requests

import sys

from pyspark.sql import SparkSession


# Today
TODAY = datetime.today().strftime("%Y-%d-%m") 

##########################################################

# HDFS parameters

def get_hdfs_params():
    hdfs_connection = BaseHook.get_connection("hdfs")
    hdfs_extras = hdfs_connection.extra_dejson

    hdfs_host = hdfs_connection.host
    hdfs_port = hdfs_connection.port
    hdfs_uri = f"http://{hdfs_host}:{hdfs_port}"

    hdfs_user = hdfs_connection.login

    # Create HDFS client
    hdfs_client = InsecureClient(hdfs_uri, user=hdfs_user)

    hdfs_params = {
        "hdfs_db_folder"  : hdfs_extras.get("hdfs_db_folder"),
        "hdfs_api_folder" : hdfs_extras.get("hdfs_api_folder")
    }

    return hdfs_client, hdfs_params

##########################################################
# DB credentials

def get_db_creds():
    db_connection = BaseHook.get_connection("pg_local")
    db_extras = db_connection.extra_dejson

    return {
                "dbname"   : db_extras.get("dbname"), 
                "user"     : db_connection.login, 
                "password" : db_connection.password, 
                "host"     : db_connection.host,
            }

##########################################################

def get_table_names():

    db_creds = get_db_creds()
    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:

            # Get all the tables names
            cursor.execute("SELECT table_name FROM information_schema.tables where table_schema='public'")

            return set(t[0] for t in cursor.fetchall())


def process_table_hdfs(table_name):

    
    db_creds = get_db_creds()

    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:

            logging.info(f"Processing DB table {table_name} to bronze")

            hdfs_client, hdfs_params = get_hdfs_params()
            hdfs_db_folder = hdfs_params.get("hdfs_db_folder")

            hdfs_client.makedirs(f'{hdfs_db_folder}')

            with hdfs_client.write(f'{hdfs_db_folder}/{TODAY}/{table_name}.csv') as output_file:
                cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", output_file)
                logging.info(f'Table {table_name} saved to {hdfs_db_folder}/{TODAY}/{table_name}.csv')


##########################################################

def get_auth_token(api_base_url, api_auth_endpoint, api_username, api_password):

    auth_url = f"{api_base_url}/{api_auth_endpoint}"
    headers = {"content-type": "application/json"}
    payload = {"username": api_username, "password": api_password}

    logging.debug(f"Sending auth request to {auth_url}")

    try:
        auth_response = requests.post(auth_url, json=payload, headers=headers)
        auth_response.raise_for_status()

        data = json.loads(auth_response.content)
        
        return data.get("access_token")

    except requests.HTTPError as e:
        logging.error(f"Auth request to {auth_url} failed with {e.args}")


def get_api_data(api_base_url, out_endpoint, jwt_token, date_to_extract):

    data_url = f"{api_base_url}/{out_endpoint}"
    headers = {"content-type": "application/json", "Authorization": jwt_token}
    payload = {"date" : date_to_extract}

    logging.debug(f"Sending API request for {date_to_extract}")

    try:
        data_response = requests.get(data_url, json=payload, headers=headers)
        data_response.raise_for_status()

        data = json.loads(data_response.content)

        return data

    except requests.HTTPError as e:
        logging.error(f"Data request to {data_url} failed with {e.args}")


def process_api_hdfs():
    
    # API parameters
    api_connection = BaseHook.get_connection("api")
    api_extras = api_connection.extra_dejson

    api_base_url = api_connection.host
    api_auth_endpoint = api_extras.get("auth_endpoint")
    api_out_endpoint = api_extras.get("out_endpoint")
    api_username = api_connection.login
    api_password = api_connection.password


    # Get auth token
    token = get_auth_token(api_base_url, api_auth_endpoint, api_username, api_password)

    if token:

        jwt_token = f"jwt {token}"

        # Request data
        data = get_api_data(api_base_url, api_out_endpoint, jwt_token, TODAY)
        
        if not data:
            logging.error("No data returned, exiting")
            sys.exit()

        # Finally process data and save to hdfs

        hdfs_client, hdfs_params = get_hdfs_params()
        hdfs_api_folder = hdfs_params.get("hdfs_api_folder")

        folder = f"{hdfs_api_folder}/{TODAY}"
        hdfs_client.makedirs(f'{folder}')

        with hdfs_client.write(f'{folder}/data.json', encoding='utf-8') as output_file:
            json.dump(data, output_file, indent=4)
            logging.info(f"API Data saved to {folder}/data.json")

###########################################
#### Silver layer processing
###########################################

def api_to_silver():

    _, hdfs_params = get_hdfs_params()
    hdfs_api_folder = hdfs_params.get("hdfs_api_folder")
    api_file = f"{hdfs_api_folder}/{TODAY}/data.json"

    # Create spark session and read data from HDFS to parquet files

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .master('local')\
        .appName("api")\
        .getOrCreate()

    api_df = spark.read.option("multiline", "true")\
                    .json(f"{api_file}")\
                    .drop('date')\
                    .orderBy("product_id")

    logging.info(f"Saving API to silver.")
    api_df.write\
        .parquet(f"/silver/api/{TODAY}/data", mode='overwrite')

def db_table_to_silver(table_name):

    _, hdfs_params = get_hdfs_params()
    hdfs_db_folder = hdfs_params.get("hdfs_db_folder")

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .master('local')\
        .appName("api")\
        .getOrCreate()
    
    table_df = spark.read.load(f'{hdfs_db_folder}/{TODAY}/{table_name}.csv'
                                , header="true"
                                , inferSchema="true"
                                , format="csv")

    logging.info(f"Saving DB table {table_name} to silver")

    table_df.write.parquet(f"/silver/db/{TODAY}/{table_name}", mode='overwrite')
