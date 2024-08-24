from pymongo import MongoClient
from django.conf import settings

def test_mongo_connection():
    # Establish the MongoDB connection directly here
    MONGO_DB_URI = "your_mongo_uri_here"
    client = MongoClient(MONGO_DB_URI)
    db = client['RQ_Analytics']
    
    # Perform your operations
    collection = db['shopifyCustomers']
    print("Connection successful")
    # Example query
    document_count = collection.count_documents({})
    print(f"Total documents: {document_count}")
