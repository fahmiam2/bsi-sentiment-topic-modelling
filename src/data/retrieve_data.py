from pymongo import MongoClient
from config import mongoDB

def connect_to_mongodb(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def retrieve_training_data_from_mongodb(mongo_uri, db_name, collection_name, max_date):
    client, collection = connect_to_mongodb(mongo_uri, db_name, collection_name)

    # Retrieve the training data (date <= max_date)
    training_data = []
    training_documents = collection.find({"reviewDatetime": {"$lte": max_date}})
    for document in training_documents:
        training_data.append(document)

    # Close the MongoDB connection
    client.close()

    return training_data

def retrieve_prediction_data_from_mongodb(mongo_uri, db_name, collection_name, min_date):
    client, collection = connect_to_mongodb(mongo_uri, db_name, collection_name)

    # Retrieve the prediction data (date > min_date)
    prediction_data = []
    prediction_documents = collection.find({"reviewDatetime": {"$gt": min_date}})
    for document in prediction_documents:
        prediction_data.append(document)

    # Close the MongoDB connection
    client.close()

    return prediction_data

if __name__ == "__main__":
    # this is the example how to proceed this script
    # Connection details
    mongo_uri = mongoDB["mongoUrl"]  # Replace with your MongoDB URI
    db_name = mongoDB["dbName"]  # Replace with your database name
    collection_name = "collectionName"  # Replace with your collection name

    # Define the date thresholds for training and prediction data
    training_max_date = "2023-05-31"
    prediction_min_date = "2023-06-01"

    # Retrieve training data from MongoDB
    training_data = retrieve_training_data_from_mongodb(mongo_uri, db_name, collection_name, training_max_date)

    # Retrieve prediction data from MongoDB
    prediction_data = retrieve_prediction_data_from_mongodb(mongo_uri, db_name, collection_name, prediction_min_date)

    # Example processing and usage of the retrieved data
    for document in training_data:
        # Access the fields in the document using dot notation
        review_app_id = document.reviewAppId
        review_datetime = document.reviewDatetime
        user_url = document.userUrl
        # Add more fields as needed

        # Process and use the retrieved training data
        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")

    # Similarly, you can process and use the retrieved prediction data
    for document in prediction_data:
        # Access the fields in the document using dot notation
        review_app_id = document.reviewAppId
        review_datetime = document.reviewDatetime
        user_url = document.userUrl
        # Add more fields as needed

        # Process and use the retrieved prediction data
        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")
