from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os


load_dotenv()

#Initialize client once to be reused
_client = None
_db = None


def get_mongo_client():
    global _client
    if _client is None:
        username = os.getenv("MONGO_USERNAME")
        password = os.getenv("MONGO_PW")
        host = os.getenv("MONGO_HOST")
        retry = os.getenv("MONGO_RETRY")
        appname = os.getenv("MONGO_APP_NAME")

        uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites={retry}&w=majority&appName={appname}"
        _client = MongoClient(uri, server_api=ServerApi('1'))
        print("Connected to MongoDB successfully.")
    return _client

def get_db():
    global _db
    if _db is None:
        _db = get_mongo_client()[os.getenv("DB_NAME")]
    return _db



def initialize_mongo_client():

    
    """
    Initializes the MongoDB client using environment variables.
    """
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PW")
    host = os.getenv("MONGO_HOST")
    retry = os.getenv("MONGO_RETRY")
    appname = os.getenv("MONGO_APP_NAME")

    uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites={retry}&w=majority&appName={appname}"
    
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    print("Connected to MongoDB successfully.")

    return client

def insert_bs_item(extracted_bs_item):

    # client = initialize_mongo_client()
    db = get_db()[os.getenv("COLLECTION_BS")]
    collection_bs = db[os.getenv("COLLECTION_BS")]

    try:
        collection_bs.insert_one(extracted_bs_item)
        print("BS item inserted successfully.")
        
    except Exception as e:
        print(e)

def insert_is_item(extracted_is_item):
    
    # client = initialize_mongo_client()
    # db = client[os.getenv("DB_NAME")]
    collection_is = get_db()[os.getenv("COLLECTION_IS")]

    try:
        collection_is.insert_one(extracted_is_item)
        print("IS item inserted successfully.")
        
    except Exception as e:
        print(e)