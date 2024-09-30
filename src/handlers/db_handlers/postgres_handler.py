# src/handlers/db_handlers/postgresql_handler.py

import psycopg2
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
from src.utils.logger import setup_logger

# Load environment variables
load_dotenv()


class PostgresHandler(BaseDBHandler):
    def __init__(self):
        super().__init__()  # Initialize the superclass
        self.connection = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        self.cursor = self.connection.cursor()
        self.logger = setup_logger()

    def save_data(self, df):
        try:
            self.logger.info("Inserting data into PostgreSQL")
            insert_query = f"INSERT INTO {os.getenv('POSTGRES_TABLE')} VALUES (%s, %s, %s)"  # Adapt as needed
            for _, row in df.iterrows():
                self.cursor.execute(insert_query, tuple(row.values))
            self.connection.commit()
            self.logger.info("Data successfully inserted into PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            self.connection.rollback()

    def insert_records(self, records):
        try:
            self.logger.info("Inserting data into PostgreSQL")
            insert_query = "INSERT INTO your_table (column1, column2, column3) VALUES (%s, %s, %s)"
            self.cursor.executemany(insert_query, records)
            self.connection.commit()
            self.logger.info("Data successfully inserted into PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            self.connection.rollback()
