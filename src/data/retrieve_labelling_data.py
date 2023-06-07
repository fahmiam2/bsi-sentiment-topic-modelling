from retrieve_data import MongoDBClient
from config import mongoDB
import json
import os
from bson import ObjectId

def connect_to_mongodb(mongo_uri, db_name, collection_name):
    mongo_client = MongoDBClient(mongo_uri, db_name, collection_name)
    return mongo_client


def retrieve_labelling_data(mongo_client, labelling_max_date):
    labelling_query = {"reviewDatetime": {"$lte": labelling_max_date}}
    labelling_data = mongo_client.retrieve_data(labelling_query)
    return labelling_data


def close_mongodb_connection(mongo_client):
    mongo_client.close_connection()

def export_data_as_json(data, file_path):
    def convert_to_json_serializable(obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return obj

    with open(file_path, "w") as file:
        json.dump(data, file, default=convert_to_json_serializable)
    print(f"Data exported as JSON: {file_path}")

def process_labelling_data(labelling_data):
    for document in labelling_data:
        review_app_id = document["reviewAppId"]
        review_datetime = document["reviewDatetime"]
        user_url = document["userUrl"]
        # Add more fields as needed

        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")


def main():
    # Connection details
    mongo_uri = mongoDB["mongoUrl"]
    db_name = mongoDB["dbName"]
    collection_name = mongoDB["collectionName"]

    # Create an instance of the MongoDBClient
    mongo_client = connect_to_mongodb(mongo_uri, db_name, collection_name)

    # Define the date thresholds for labelling
    labelling_max_date = "2023-05-31"

    # Retrieve labelling data from MongoDB
    labelling_data = retrieve_labelling_data(mongo_client, labelling_max_date)

    # Close the MongoDB connection
    close_mongodb_connection(mongo_client)

    # Process and use the retrieved labelling data
    process_labelling_data(labelling_data)

    # Export labelling data as JSON
    file_path = r"/app/data/raw/labelling_data.json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    export_data_as_json(labelling_data, file_path)

# Execute the main function
if __name__ == "__main__":
    main()