# src/handlers/db_handlers/postgresql_handler.py

import psycopg2
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
from src.utils.logger import setup_logger

# Load environment variables
load_dotenv()


class PostgresHandler(BaseDBHandler):
    """
    Manages database operations specifically for PostgreSQL.

    This handler includes functionalities to connect to and interact with PostgreSQL databases,
    focusing on operations like inserting data from DataFrames or direct record lists.

    :param BaseDBHandler: Inherits logging and basic database interaction functionalities.
    """

    def __init__(self):
        """
        Initializes the PostgresHandler by setting up a database connection using environment variables.
        """
        super().__init__()
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
        """
        Inserts data from a DataFrame into a PostgreSQL database.

        :param df: DataFrame containing data to be inserted.
        :type df: pandas.DataFrame
        """
        try:
            self.logger.info("Inserting data into PostgreSQL")
            insert_query = f"INSERT INTO {os.getenv('POSTGRES_TABLE')} VALUES (%s, %s, %s)"
            for _, row in df.iterrows():
                self.cursor.execute(insert_query, tuple(row.values))
            self.connection.commit()
            self.logger.info("Data successfully inserted into PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            self.connection.rollback()

    def insert_records(self, records):
        """
        Performs bulk insert of multiple records into a PostgreSQL table.

        :param records: List of records to be inserted, where each record is a tuple of values.
        :type records: list of tuples
        """
        try:
            self.logger.info("Inserting data into PostgreSQL")
            insert_query = "INSERT INTO your_table (column1, column2, column3) VALUES (%s, %s, %s)"
            self.cursor.executemany(insert_query, records)
            self.connection.commit()
            self.logger.info("Data successfully inserted into PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            self.connection.rollback()
