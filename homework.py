import requests
import json
import logging
import sys
import os

import psycopg2


CONFIG_FILE="config.json"

# Setup logging
logger = logging.Logger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def read_config(config_filename):
    ''' Read config file, return json with parameters
    '''

    try:
        with open(config_filename, "r") as config_file:
            config_data = json.load(config_file)
            return config_data

    except FileNotFoundError:
        logger.error("Config file do not exists, exiting.")
   
def get_auth_token(base_url, auth_endpoint, content_type, username, password):
    '''Sends auth POST request, returns auth token
    '''

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

def dump_api_to_disk():
    
    config_data = read_config(CONFIG_FILE)

    if not config_data:
        sys.exit()

    # Get parameters
    base_url = config_data.get("base_url")
    auth_endpoint = config_data.get("auth_endpoint")
    out_endpoint = config_data.get("out_endpoint")
    auth_type = config_data.get("auth_type")
    content_type = config_data.get("content_type")
    username = config_data.get("username")
    password = config_data.get("password")
    date_to_extract = config_data.get("date_to_extract")
    
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


def dump_db_to_disk():

    from contextlib import closing

    config_data = read_config(CONFIG_FILE)

    if not config_data:
        sys.exit()

    db_dump_folder = config_data.get("db_dump_folder")
    dbname = config_data.get("dbname")
    db_user = config_data.get("db_user")
    db_password = config_data.get("db_password")
    db_host = config_data.get("db_host")
 
    if not os.path.exists(db_dump_folder):
        os.makedirs(db_dump_folder)

    db_creds = {
                "dbname"   : dbname, 
                "user"     : db_user, 
                "password" : db_password, 
                "host"     : "localhost"}

    with closing(psycopg2.connect(**db_creds)) as conn:
        with conn.cursor() as cursor:
            # Get all the tables names
            cursor.execute("SELECT table_name FROM information_schema.tables where table_schema='public'")

            tables = cursor.fetchall()
            for table_name in tables:
                table_name = table_name[0]
                logger.debug(f"Processing table {table_name}")

                with open(f"{db_dump_folder}/{table_name}", "w+") as output_file:   
                    cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", output_file)
 

############################################

if __name__ == "__main__":
    dump_api_to_disk()
    dump_db_to_disk()