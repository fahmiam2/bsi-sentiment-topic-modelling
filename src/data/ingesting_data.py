from pymongo import MongoClient
from config import mongoDB

class MongoDBClient:
    def __init__(self, mongo_uri, db_name, collection_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def retrieve_data(self, query):
        documents = self.collection.find(query)
        return list(documents)

    def insert_data(self, data):
        self.collection.insert_many(data)

    def close_connection(self):
        self.client.close()

if __name__ == "__main__":
    # an Example to use this class object
    # Connection details
    mongo_uri = mongoDB["mongoUrl"]  # Replace with your MongoDB URI
    db_name = mongoDB["dbName"]  # Replace with your database name
    collection_name = mongoDB["collectionName"]  # Replace with your collection name

    # Create an instance of the MongoDBClient
    mongo_client = MongoDBClient(mongo_uri, db_name, collection_name)

    # Example data to be inserted
    new_data = [
        {"field1": 1, "field2": 4},  # Document 1
        {"field1": 2, "field2": 3},  # Document 2
        # Add more documents as needed
    ]

    # Insert the data into MongoDB
    mongo_client.insert_data(new_data)

    # Close the MongoDB connection
    mongo_client.close_connection()


