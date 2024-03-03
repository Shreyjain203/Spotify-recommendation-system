import pymongo
import json

def save_mongo_data(data, local=True):
    """
    Save data 
    """
    if local:
        file_path = 'mongo_aggregation.json'
        with open(file_path, 'w') as file:
            for doc in data:
                file.write(json.dumps(doc, indent=4))
                file.write('\n')
    else:
        # TODO: Implement GCP upload
        pass


def load_json_data(client_url: str, db_name: str, collection_name: str) -> None:
    """For local use only, run once manually to load historical data"""
    client = pymongo.MongoClient(client_url)
    db = client[db_name]
    collection = db[collection_name]
    # empty collection first
    collection.drop()

    file_path = 'historical_data.json' # Change this to your file path
    with open(file_path, 'r') as file:
        data = json.load(file)
        collection.insert_many(data)


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

    feature_extraction_pipeline = [
        # {<song_title>: {features}} -> {features}
        {
            '$replaceRoot': {
                'newRoot': { '$arrayElemAt': [
                    {'$objectToArray': '$$ROOT'}, 1
                ]}
            }
        },
        {'$replaceRoot': {'newRoot': '$v'}},

        # Get avgs
        {'$group': avg_group}
    ]

    result = collection.aggregate(feature_extraction_pipeline)
    save_mongo_data(result)


if __name__ == '__main__':
    mongo_aggregation(
        client_url='mongodb://localhost:27017/',
        db_name='test',
        collection_name='test'
    )
