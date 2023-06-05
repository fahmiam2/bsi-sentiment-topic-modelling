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

    def close_connection(self):
        self.client.close()

if __name__ == "__main__":
    # This is the example how to proceed this script
    # Connection details
    mongo_uri = mongoDB["mongoUrl"]  # Replace with your MongoDB URI
    db_name = mongoDB["dbName"]  # Replace with your database name
    collection_name = mongoDB["collectionName"]  # Replace with your collection name

    # Create an instance of the MongoDBClient
    mongo_client = MongoDBClient(mongo_uri, db_name, collection_name)

    # Define the date thresholds for training and prediction data
    training_max_date = "2023-05-31"
    prediction_min_date = "2023-06-01"

    # Retrieve training data from MongoDB
    training_query = {"reviewDatetime": {"$lte": training_max_date}}
    training_data = mongo_client.retrieve_data(training_query)

    # Retrieve prediction data from MongoDB
    prediction_query = {"reviewDatetime": {"$gt": prediction_min_date}}
    prediction_data = mongo_client.retrieve_data(prediction_query)

    # Close the MongoDB connection
    mongo_client.close_connection()

    # Process and use the retrieved training data
    for document in training_data:
        review_app_id = document["reviewAppId"]
        review_datetime = document["reviewDatetime"]
        user_url = document["userUrl"]
        # Add more fields as needed

        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")

    # Similarly, process and use the retrieved prediction data
    for document in prediction_data:
        review_app_id = document["reviewAppId"]
        review_datetime = document["reviewDatetime"]
        user_url = document["userUrl"]
        # Add more fields as needed

        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")
