from flows.mongo_flow import load_json_data, mongo_clean_data, mongo_aggregation
from flows.spark_flow import load_data, spark_df, get_spark_session
from flows.model_flow import train_model

# TODO: Download spotify data into `data/spotify_data.json`

mongo_params = {
    "client_url": "mongodb://localhost:27017/",
    "db_name": "test",
    "collection_name": "test"
}
save_path = "feature_data.json"

load_json_data(**mongo_params, file_path="spotify_data.json")
mongo_clean_data(**mongo_params, file_path=save_path)
mongo_aggregation(**mongo_params)

feature_save_path = "model_features.json"

sc = get_spark_session(name = "Spotify_Recommendation")
spark_df(sc, feature_save_path)

train_model(feature_save_path, 10)
