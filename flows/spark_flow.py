from pyspark.sql import SparkSession
import json

def load_data(local=True):
    """ TODO: Add functionality to load data from GCP """
    if local:
        file_path = 'feature_data.json'
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    else:
        print('NOT IMPLEMENTED :: Load data from GCP')
        return


def save_data(data, file_path:str, local=True):
    if local:
        with open(file_path, 'w') as file:
            file.write(json.dumps(data, indent=4))
    else:
        print('NOT IMPLEMENTED :: Save data to GCP')
        pass

def spark_df(spark: SparkSession, file_path: str):
    """
    Create a Spark DataFrame from JSON data and save a formatted dataset
    """
    data = load_data()
    df = spark.createDataFrame(data)
    
    df.createOrReplaceTempView("songs")
    df_lag = spark.sql(
        "SELECT song_id, LAG(song_id, 1) OVER (ORDER BY song_id) as prev_song_id FROM songs"
    )

    df_lag = df_lag.filter(df_lag["prev_song_id"].isNotNull())
    
    save_data(df_lag.collect(), file_path)

def get_spark_session(name: str = "SparkDF"):
    return SparkSession.builder.appName(name).getOrCreate()

if __name__ == "__main__":
    sc = SparkSession.builder.appName("SparkDF").getOrCreate()
    spark_df(sc, "spark_features.json")
    # print(f"Count of items in DataFrame: {count}")
