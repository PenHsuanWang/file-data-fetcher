# src/handlers/db_handlers/mongo_handler.py

import pymongo
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
import pandas as pd

# Load environment variables from the .env file
load_dotenv()


class MongoDBHandler(BaseDBHandler):
    """
    Handles MongoDB database operations, specifically for inserting records.

    This class manages connections to MongoDB and implements data insertion both from DataFrames
    and direct lists of dictionary records.

    :param BaseDBHandler: Inherits from BaseDBHandler for logging and common database functionalities.
    """

    def __init__(self):
        """
        Initializes the MongoDBHandler by setting up a connection to MongoDB using the provided URI.
        """
        super().__init__()
        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        mongo_database = os.getenv("MONGODB_DATABASE", "mydatabase")
        mongo_collection = os.getenv("MONGODB_COLLECTION", "mycollection")

        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_database]
        self.collection = self.db[mongo_collection]

    def insert_records(self, records):
        """
        Inserts a list of dictionary records into the specified MongoDB collection.

        :param records: List of dictionaries representing the records to be inserted.
        :type records: list of dict
        """
        try:
            self.logger.info(f"Inserting data into MongoDB collection {self.collection.name}")
            self.collection.insert_many(records)
            self.logger.info("Data successfully inserted into MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to insert data into MongoDB: {str(e)}")
            raise

    def save_data(self, df):
        """
        Converts a pandas DataFrame into dictionary records and inserts them into MongoDB.

        :param df: DataFrame to be converted and inserted into the database.
        :type df: pandas.DataFrame
        """
        records = df.to_dict(orient="records")
        self.insert_records(records)
