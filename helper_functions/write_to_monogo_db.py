# !pip install pymongo
from pymongo import MongoClient

def upload_data_to_mongo(data, connection_string, database_name, collection_name):
  """
  Uploads data to a MongoDB collection using the provided connection string.

  Args:
      data: The data to upload (list of dictionaries or other serializable format).
      connection_string: The MongoDB connection string.
      database_name: The name of the database to access.
      collection_name: The name of the collection to insert data into.

  Returns:
      None
  """

  # Connect to MongoDB using the connection string
  client = MongoClient(connection_string)
  
  # Get the database object
  db = client[database_name]
  
  # Get the collection object
  collection = db[collection_name]
  
  # Insert the data into the collection
  collection.insert_many(data)
  
  # Close the connection
  client.close()
