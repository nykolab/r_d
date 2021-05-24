import json
import logging
import sys
import os
import requests
import psycopg2
from contextlib import closing

from hdfs import InsecureClient
from datetime import datetime

from airflow.hooks.base_hook import BaseHook

import concurrent.futures

# Setup logging
logger = logging.Logger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

TODAY = datetime.today().strftime("%m_%d_%Y") 

# Read config
CONFIG_FILE="config.json"

try:
    with open(CONFIG_FILE, "r") as config_file:
        config_data = json.load(config_file)

except FileNotFoundError:
    logger.error("Config file do not exists, exiting.")
    sys.exit()

# DB credentials
connection = BaseHook.get_connection("pg_local")

extras = connection.extra_dejson

db_creds = {
                "dbname"   : extras.get("dbname"), 
                "user"     : connection.login, 
                "password" : connection.password, 
                "host"     : connection.host,
            }

# Folder on disk to dump DB
db_dump_folder : extras.get("db_dump_folder")

# HDFS parameters
hdfs_uri = config_data.get("hdfs_uri")
hdfs_user = config_data.get("hdfs_user")
hdfs_db_folder = config_data.get("hdfs_db_folder")
hdfs_api_folder = config_data.get("hdfs_api_folder")

# Create HDFS client
client = InsecureClient(hdfs_uri, user=hdfs_user)
client.makedirs(f'{hdfs_db_folder}')

# API parameters
base_url = config_data.get("base_url")
auth_endpoint = config_data.get("auth_endpoint")
out_endpoint = config_data.get("out_endpoint")
auth_type = config_data.get("auth_type")
content_type = config_data.get("content_type")
username = config_data.get("username")
password = config_data.get("password")
date_to_extract = config_data.get("date_to_extract")

############################################

def get_table_names():

    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:

            # Get all the tables names
            cursor.execute("SELECT table_name FROM information_schema.tables where table_schema='public'")

            return set(t[0] for t in cursor.fetchall())

def get_auth_token(base_url, auth_endpoint, content_type, username, password):

    auth_url = f"{base_url}/{auth_endpoint}"
    headers = {"content-type": content_type}
    payload = {"username": username, "password": password}

    logger.debug(f"Sending auth request to {auth_url}")

    try:
        auth_response = requests.post(auth_url, json=payload, headers=headers)
        auth_response.raise_for_status()

        data = json.loads(auth_response.content)
        
        return data.get("access_token")

    except requests.HTTPError as e:
        logger.error(f"Auth request to {auth_url} failed with {e.args}")

def get_api_data(base_url, out_endpoint, jwt_token, content_type, date_to_extract):
    '''Get data from the API for specified day(s)
    '''
    data_url = f"{base_url}/{out_endpoint}"
    headers = {"content-type": content_type, "Authorization": jwt_token}
    payload = date_to_extract

    logger.debug(f"Sending data request to {data_url}")

    try:
        data_response = requests.get(data_url, json=payload, headers=headers)
        data_response.raise_for_status()

        data = json.loads(data_response.content)

        return data

    except requests.HTTPError as e:
        logger.error(f"Data request to {data_url} failed with {e.args}")

def process_api_hdfs():
    
    api_data = get_api_data(config_data)






    # Get auth token
    token = get_auth_token(base_url, auth_endpoint, content_type, username, password)

    if token:
        jwt_token = f"{auth_type} {token}"

        # Request data
        data = get_api_data(base_url, out_endpoint, jwt_token, content_type, date_to_extract)
        
        if not data:
            logger.error("No data returned, exiting")
            sys.exit()

        # Finally process data and save to hdfs
        date = date_to_extract.get("date")
        folder = f"{hdfs_api_folder}/{date}"

        with client.write(f'{folder}/data.json', encoding='utf-8') as output_file:
            json.dump(data, output_file, indent=4)
            logger.debug(f"Data saved to {folder}/data.json")

def process_api_disk():

    # Get auth token
    token = get_auth_token(base_url, auth_endpoint, content_type, username, password)

    if token:
        jwt_token = f"{auth_type} {token}"

        # Request data
        data = get_api_data(base_url, out_endpoint, jwt_token, content_type, date_to_extract)
        
        if not data:
            logger.error("No data returned, exiting")
            sys.exit()

        # Finally process data and save to file
        date = date_to_extract.get("date")
        folder = f"/home/user/Data/data_eng_homework/api_data/{date}"

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(f"{folder}/data.json", "w+") as output_file:
            json.dump(data, output_file, indent=4)
            logger.debug(f"Data saved to {folder}/data.json")


def process_table_hdfs(table_name):

    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:

            logger.debug(f"Processing table {table_name}")

            with client.write(f'{hdfs_db_folder}/{TODAY}/{table_name}.csv') as output_file:
                cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", output_file)


def process_table_disk(table_name):

    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:

            logger.debug(f"Processing table {table_name}")

            with open(f"{db_dump_folder}/{TODAY}/{table_name}.csv", "w+") as output_file:   
                cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", output_file)

############################################

if __name__ == "__main__":

    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.map(process_table_hdfs, get_table_names())
    process_api_disk()
