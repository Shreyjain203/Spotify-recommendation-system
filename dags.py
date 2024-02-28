from helper_functions import get_user_data, get_data_dump, upload_to_gcs, write_to_monogo_db, read_from_gcs

user_data = get_user_data.get_user_data()
data_dump = get_data_dump.get_data_dump()

upload_to_gcs.upload_to_gcs(user_data, filename = "historical_data.json")
upload_to_gcs.upload_to_gcs(data_dump, filename = "data_dump.json")

connection_string = ""
database_name = "spotify_recommenation"
collection_name = "user_data"
user_data = read_from_gcs.read_from_gcs(filename = "historical_data.json")
write_to_monogo_db.upload_data_to_mongo(user_data, connection_string, database_name, collection_name)

connection_string = ""
database_name = "spotify_recommenation"
collection_name = "data_dump"
data_dump = read_from_gcs.read_from_gcs(filename = "data_dump.json")
write_to_monogo_db.upload_data_to_mongo(data_dump, connection_string, database_name, collection_name)