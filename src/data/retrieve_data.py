from pymongo import MongoClient
from config import mongoDB

def retrieve_data_from_mongodb(mongo_uri, db_name, collection_name):
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Retrieve all documents from the collection
    documents = collection.find()

    # Process and return the retrieved data
    retrieved_data = []
    for document in documents:
        retrieved_data.append(document)

    # Close the MongoDB connection
    client.close()

    return retrieved_data

if __name__ == "__main__":
    # Connection details
    mongo_uri = mongoDB["mongoUrl"]  # Replace with your MongoDB URI
    db_name = mongoDB["dbName"]  # Replace with your database name
    collection_name = ["collectionName"]  # Replace with your collection name

    # Retrieve data from MongoDB
    data = retrieve_data_from_mongodb(mongo_uri, db_name, collection_name)

    # Example process and use the retrieved data as needed
    for document in data:
        # Access the fields in the document
        review_app_id = document['reviewAppId']
        review_datetime = document['reviewDatetime']
        user_url = document['userUrl']
        # Add more fields as needed

        # Process and use the retrieved data
        # For example, print the fields
        print(f"Review App ID: {review_app_id}")
        print(f"Review Datetime: {review_datetime}")
        print(f"User URL: {user_url}")
        print("--------------------------------")