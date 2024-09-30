# src/handlers/db_handlers/snowflake_handler.py

import snowflake.connector
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
from src.utils.logger import setup_logger

# Load environment variables
load_dotenv()


class SnowflakeHandler(BaseDBHandler):
    """
    Handles interactions with Snowflake databases for data insertion.

    Extends the `BaseDBHandler` to provide specific functionality for connecting to and
    operating with Snowflake databases. It supports inserting data both from pandas DataFrame
    and from lists of records.

    :param BaseDBHandler: Provides initial logging setup and common handler functionalities.
    """

    def __init__(self):
        """
        Initializes the SnowflakeHandler with a connection to Snowflake using environment variables.
        """
        super().__init__()
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        self.logger = setup_logger()

    def save_data(self, df):
        """
        Saves data from a pandas DataFrame to Snowflake.

        :param df: DataFrame containing data to be inserted into Snowflake.
        :type df: pandas.DataFrame
        """
        try:
            self.logger.info("Inserting data into Snowflake")
            insert_query = f"INSERT INTO {os.getenv('SNOWFLAKE_TABLE')} VALUES (%s, %s, %s)"
            cursor = self.conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row.values))
            cursor.close()
            self.conn.commit()
            self.logger.info("Data successfully inserted into Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to insert data into Snowflake: {str(e)}")

    def insert_records(self, records):
        """
        Inserts a list of records into a Snowflake table.

        :param records: List of tuples, each tuple containing data for one record.
        :type records: list of tuples
        """
        try:
            self.logger.info("Inserting data into Snowflake")
            cursor = self.conn.cursor()
            insert_query = "INSERT INTO your_table (column1, column2, column3) VALUES (%s, %s, %s)"
            cursor.executemany(insert_query, records)
            cursor.close()
            self.conn.commit()
            self.logger.info("Data successfully inserted into Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to insert data into Snowflake: {str(e)}")