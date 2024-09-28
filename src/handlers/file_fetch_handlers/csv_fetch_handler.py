# src/handlers/file_handlers/csv_fetch_handler.py

import pandas as pd
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileHandler
from logger import Logger


class CSVHandler(BaseFileHandler):
    def __init__(self, file_path):
        self.file_path = file_path
        self.logger = Logger().get_logger()

    def process_file(self):
        try:
            # Read CSV file into a pandas DataFrame
            self.logger.info(f"Processing CSV file: {self.file_path}")
            df = pd.read_csv(self.file_path)
            return df
        except Exception as e:
            self.logger.error(f"Error processing CSV file {self.file_path}: {str(e)}")
            return None

