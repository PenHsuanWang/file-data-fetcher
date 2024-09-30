# src/handlers/db_handlers/mongo_handler.py

import pymongo
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
import pandas as pd

# Load environment variables from the .env file
load_dotenv()


class MongoDBHandler(BaseDBHandler):
    def __init__(self):
        # Call the base class to initialize the logger
        super().__init__()

        # Fetch MongoDB URI, database, and collection name from environment variables
        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        mongo_database = os.getenv("MONGODB_DATABASE", "mydatabase")
        mongo_collection = os.getenv("MONGODB_COLLECTION", "mycollection")
        
        # Initialize MongoDB client
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_database]
        self.collection = self.db[mongo_collection]

    def insert_records(self, records):
        """
        Inserts records into the MongoDB collection.
        """
        try:
            # Logging insert operation
            self.logger.info(f"Inserting data into MongoDB collection {self.collection.name}")
            self.collection.insert_many(records)
            self.logger.info("Data successfully inserted into MongoDB")
        except Exception as e:
            # Log the error in case of failure
            self.logger.error(f"Failed to insert data into MongoDB: {str(e)}")
            raise

    def save_data(self, df: pd.DataFrame):
        """
        Convert a DataFrame to a list of dictionary records and save it to MongoDB.
        """
        records = df.to_dict(orient="records")
        self.insert_records(records)
