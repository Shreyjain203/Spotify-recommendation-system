# Spotify Recommendation System
Built a Spotify recommendation system: fetched data from Spotify API, stored it on Google Cloud Storage, orchestrated with Airflow (Composer) to load into MongoDB Atlas, and developed a recommendation engine based on the data.

## Usage
```bash
$ pip install -r requirements.txt
# Run locally
$ python local_dag.py
# Run without GCS support
$ python dag.py
```
