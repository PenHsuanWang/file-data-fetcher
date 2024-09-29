# src/handlers/file_handlers/csv_fetch_handler.py

import pandas as pd
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler


class CSVFetchHandler(BaseFileFetchHandler):
    """
    Handler class for processing CSV files.

    :param processed_registry: The registry used to track processed files.
    :type processed_registry: ProcessedFilesRegistry
    """
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a CSV file and return a pandas DataFrame.

        :param file_path: The path to the CSV file.
        :type file_path: str
        :return: A pandas DataFrame containing the content of the CSV file.
        :rtype: pd.DataFrame
        """
        return pd.read_csv(file_path)
