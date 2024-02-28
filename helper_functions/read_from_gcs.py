from pymongo import MongoClient
from google.cloud import storage
import json

def read_from_gcs(file_name):
    # Replace with your project ID and bucket name
    project_id = "decoded-nebula-283211"
    bucket_name = "msds-697"

    # Create a client object
    client = storage.Client(project=project_id)

    # Create a bucket object
    bucket = client.get_bucket(bucket_name)

    # Create a blob object (file) within the bucket
    blob = bucket.blob(file_name)

    # Download the data to the blob
    json_data_string = blob.download_as_string()

    data = json.loads(json_data_string)
    return data