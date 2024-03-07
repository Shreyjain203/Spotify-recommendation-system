from flows.mongo_flow import load_json_data, mongo_clean_data, mongo_aggregation
from flows.spark_flow import load_data, spark_df, get_spark_session

mongo_params = {
    "client_url": "mongodb://localhost:27017/",
    "db_name": "test",
    "collection_name": "test"
}
save_path = "feature_data.json"

load_json_data(**mongo_params, file_path="data/spotify_data.json")
mongo_clean_data(**mongo_params, file_path=save_path)
load_json_data(**mongo_params, file_path=save_path)
mongo_aggregation(**mongo_params)

sc = get_spark_session(name = "Spotify_Recommendation")
spark_df(sc)
