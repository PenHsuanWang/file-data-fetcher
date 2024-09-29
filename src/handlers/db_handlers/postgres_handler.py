# src/handlers/db_handlers/postgres_handler.py

import psycopg2
import os
from dotenv import load_dotenv
from base_db_handler import BaseDBHandler
from logger import Logger

# Load environment variables
load_dotenv()

class PostgresHandler(BaseDBHandler):
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        self.cursor = self.connection.cursor()
        self.logger = Logger().get_logger()

    def save_data(self, df):
        try:
            self.logger.info("Inserting data into PostgreSQL")
            for index, row in df.iterrows():
                self.cursor.execute(
                    f"INSERT INTO {os.getenv('POSTGRES_TABLE')} VALUES ({', '.join(map(str, row.values))})"
                )
            self.connection.commit()
            self.logger.info("Data successfully inserted into PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            self.connection.rollback()
