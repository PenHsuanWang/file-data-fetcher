# src/handlers/db_handlers/snowflake_handler.py

import snowflake.connector
import os
from dotenv import load_dotenv
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
from src.utils.logger import setup_logger

# Load environment variables
load_dotenv()


class SnowflakeHandler(BaseDBHandler):
    def __init__(self):
        super().__init__()  # Initialize the superclass
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
        try:
            self.logger.info("Inserting data into Snowflake")
            insert_query = f"INSERT INTO {os.getenv('SNOWFLAKE_TABLE')} VALUES (%s, %s, %s)"  # Adapt as needed
            cursor = self.conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row.values))
            cursor.close()
            self.conn.commit()
            self.logger.info("Data successfully inserted into Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to insert data into Snowflake: {str(e)}")

    def insert_records(self, records):
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