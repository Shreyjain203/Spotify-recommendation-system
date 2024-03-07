import pymongo
import json

def save_mongo_data(data, output_path:str, local=True):
    """
    Save data 
    """
    if local:
        with open(output_path, 'w') as file:
            file.write(json.dumps(data, indent=4))
    else:
        # TODO: Implement GCP upload
        pass


def load_json_data(client_url: str, db_name: str, collection_name: str, file_path: str) -> None:
    """For local use only, run once manually to load historical data"""
    client = pymongo.MongoClient(client_url)
    db = client[db_name]
    collection = db[collection_name]
    collection.drop()

    with open(file_path, 'r') as file:
        data = json.load(file)
        collection.insert_many(data)


def mongo_clean_data(client_url: str, db_name: str, collection_name: str, file_path:str) -> None:
    client = pymongo.MongoClient(client_url)
    db = client[db_name]
    collection = db[collection_name]

    # {<song_title>: {features}} -> {features}
    feature_extraction_pipeline = [
        {
            '$replaceRoot': {
                'newRoot': { '$arrayElemAt': [
                    {'$objectToArray': '$$ROOT'}, 1
                ]}
            }
        },
        {'$replaceRoot': {'newRoot': '$v'}},
    ]

    result = collection.aggregate(feature_extraction_pipeline)

    save_mongo_data(list(result), file_path)


def mongo_aggregation(client_url: str, db_name: str, collection_name: str) -> None:
    client = pymongo.MongoClient(client_url)
    db = client[db_name]
    collection = db[collection_name]

    avg_features = [
        'acousticness', 'danceability', 'energy', 'instrumentalness',
        'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'key'
    ]

    avg_group = {}
    avg_group['_id'] = None
    for feature in avg_features:
        avg_group[f"average_{feature}"] = {'$avg': f'$audio_features.{feature}'}

    avg_pipeline = [ {'$group': avg_group} ]
    averages = collection.aggregate(avg_pipeline)
    averages = list(averages)[0] # Make avgs json serializable
    save_mongo_data(averages, 'mongo_averages.json')


if __name__ == '__main__':
    mongo_params = {
        'client_url': 'mongodb://localhost:27017/',
        'db_name': 'test',
        'collection_name': 'test'
    }

    load_json_data(**mongo_params, file_path='historical_data.json')
    fPath = 'feature_data.json'
    mongo_clean_data(**mongo_params, output_path=fPath)
    load_json_data(**mongo_params, file_path=fPath)
    mongo_aggregation(**mongo_params)

