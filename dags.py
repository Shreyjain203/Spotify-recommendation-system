from helper_functions import get_user_data, get_data_dump, upload_to_gcs, write_to_monogo_db, read_from_gcs

import json

import time
import datetime

from pymongo import MongoClient
from google.cloud import storage

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_data():
    user_data = get_user_data.get_user_data()
    data_dump = get_data_dump.get_data_dump()

    upload_to_gcs.upload_to_gcs(user_data, filename = "historical_data.json")
    upload_to_gcs.upload_to_gcs(data_dump, filename = "data_dump.json")

def upload_data():
    # Reading data from GCS
    connection_string = ""
    database_name = "spotify_recommenation"
    collection_name = "user_data"
    user_data = read_from_gcs.read_from_gcs(filename = "historical_data.json")
    write_to_monogo_db.upload_data_to_mongo(user_data, connection_string, database_name, collection_name)

    #Uploading data to GCS
    connection_string = ""
    database_name = "spotify_recommenation"
    collection_name = "data_dump"
    data_dump = read_from_gcs.read_from_gcs(filename = "data_dump.json")
    write_to_monogo_db.upload_data_to_mongo(data_dump, connection_string, database_name, collection_name)

MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"

with DAG(dag_id="msds697", schedule_interval='@daily', start_date=datetime(2024, 1, 1)) as dag:
    get_data_dag = PythonOperator(
        task_id="get_data_dag",
        python_callable=get_data,
    )

    upload_data_dag = PythonOperator(
        task_id="upload_data_dag",
        python_callable=upload_data
    )

    get_data_dag >> upload_data_dag
