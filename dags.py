from helper_functions import get_user_data, get_data_dump, upload_to_gcs, write_to_monogo_db, read_from_gcs
from flows.mongo_flow import load_json_data, mongo_clean_data, mongo_aggregation
from flows.spark_flow import load_data, spark_df, get_spark_session
from flows.model_flow import train_model

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
    database_name = "spotify_recommendation"
    collection_name = "user_data"
    user_data = read_from_gcs.read_from_gcs(filename = "historical_data.json")
    write_to_monogo_db.upload_data_to_mongo(user_data, connection_string, database_name, collection_name)

    #Uploading data to GCS
    connection_string = ""
    database_name = "spotify_recommendation"
    collection_name = "data_dump"
    data_dump = read_from_gcs.read_from_gcs(filename = "data_dump.json")
    write_to_monogo_db.upload_data_to_mongo(data_dump, connection_string, database_name, collection_name)

def mongo_flow():
    mongo_params = {
        "client_url": "mongodb://localhost:27017/",
        "db_name": "spotify_recomendation",
        "collection_name": "user_data"
    }
    save_path = "feature_data.json"
    
    load_json_data(**mongo_params, file_path="spotify_data.json")
    mongo_clean_data(**mongo_params, file_path=save_path)
    mongo_aggregation(**mongo_params)

def spark_flow():
    feature_save_path = "model_features.json"

    sc = get_spark_session(name = "Spotify_Recommendation")
    spark_df(sc, feature_save_path)

def model_flow():
    train_model(feature_save_path, 10)

with DAG(dag_id="msds697", schedule_interval='@daily', start_date=datetime(2024, 1, 1)) as dag:
    get_data_dag = PythonOperator(
        task_id="get_data_dag",
        python_callable=get_data,
    )

    upload_data_dag = PythonOperator(
        task_id="upload_data_dag",
        python_callable=upload_data
    )

    mongo_dag = PythonOperator(
        task_id="mongo_dag",
        python_callable=mongo_flow
    )

    spark_dag = PythonOperator(
        task_id="spark_dag",
        python_callable=spark_flow
    )

    model_dag = PythonOperator(
        task_id="model_dag",
        python_callable=model_flow
    )

    get_data_dag >> upload_data_dag >> mongo_dag >> spark_dag >> model_dag
