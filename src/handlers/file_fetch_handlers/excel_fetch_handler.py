# src/handlers/file_handlers/excel_fetch_handler.py

import pandas as pd
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler

class ExcelFetchHandler(BaseFileFetchHandler):
    def read_file(self, file_path):
        """Read an Excel file and return a DataFrame."""
        return pd.read_excel(file_path)
