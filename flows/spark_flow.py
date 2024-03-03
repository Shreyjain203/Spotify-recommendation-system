from pyspark.sql import SparkSession
import json

def load_data(local=True):
    """ TODO: Add functionality to load data from GCP """
    if local:
        file_path = 'historical_data.json'
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    else:
        print('NOT IMPLEMENTED :: Load data from GCP')
        return


def spark_df(spark: SparkSession):
    """
    Create a Spark DataFrame from JSON data and return the count of items
    """
    data = load_data()
    df = spark.createDataFrame(data)
    
    df.createOrReplaceTempView("audio_features")
    count = spark.sql("SELECT COUNT(*) FROM audio_features").collect()[0][0]
    return count

if __name__ == "__main__":
    sc = SparkSession.builder.appName("SparkDF").getOrCreate()
    count = spark_df(sc)
    print(f"Count of items in DataFrame: {count}")
