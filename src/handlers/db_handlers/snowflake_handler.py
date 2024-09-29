# src/handlers/db_handlers/snowflake_handler.py

import snowflake.connector
import os
from dotenv import load_dotenv
from base_db_handler import BaseDBHandler
from logger import Logger

# Load environment variables
load_dotenv()

class SnowflakeHandler(BaseDBHandler):
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        self.logger = Logger().get_logger()

    def save_data(self, df):
        try:
            self.logger.info("Inserting data into Snowflake")
            cursor = self.conn.cursor()
            for index, row in df.iterrows():
                cursor.execute(
                    f"INSERT INTO {os.getenv('SNOWFLAKE_TABLE')} VALUES ({', '.join(map(str, row.values))})"
                )
            cursor.close()
            self.conn.commit()
            self.logger.info("Data successfully inserted into Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to insert data into Snowflake: {str(e)}")
