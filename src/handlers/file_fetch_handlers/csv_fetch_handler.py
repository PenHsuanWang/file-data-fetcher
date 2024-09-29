# src/handlers/file_handlers/csv_fetch_handler.py

import pandas as pd
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler


class CSVFetchHandler(BaseFileFetchHandler):
    def read_file(self, file_path):
        """Read a CSV file and return a DataFrame."""
        return pd.read_csv(file_path)
