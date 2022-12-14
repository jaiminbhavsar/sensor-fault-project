import pymongo
from sensor.constant.database import DATABASE_NAME
#from sensor.constant.env_variable import MONGODB_URL_KEY
import certifi
import os
ca = certifi.where()

class MongoDBClient:
    client = None
    #MONGODB_URL_KEY = 'mongodb+srv://iNeuronDatabase:1234@jaiminml.b6rcy.mongodb.net/?retryWrites=true&w=majority'
    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:

            if MongoDBClient.client is None:
                #mongo_db_url = os.getenv(MONGODB_URL_KEY)
                mongo_db_url = "mongodb+srv://iNeuronDatabase:12345@jaiminml.b6rcy.mongodb.net/?retryWrites=true&w=majority"
                #mongo_db_url = "mongodb://localhost:27017/"
                print(mongo_db_url)
                if "localhost" in mongo_db_url:
                    MongoDBClient.client = pymongo.MongoClient(mongo_db_url) 
                else:
                    MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
        except Exception as e:
            raise e





