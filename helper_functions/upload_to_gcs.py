from google.cloud import storage
import json

def upload_to_gcs(song_meta_data, filename):
    # Replace with your project ID and bucket name
    project_id = "decoded-nebula-283211"
    bucket_name = "msds-697"

    # Define the data to write (can be string, bytes, or file path)
    data = json.dumps(song_meta_data, indent=4)

    # Create a client object
    client = storage.Client(project=project_id)

    # Create a bucket object
    bucket = client.bucket(bucket_name)

    # Create a blob object (file) within the bucket
    blob = bucket.blob(filename)

    # Upload the data to the blob
    blob.upload_from_string(data)