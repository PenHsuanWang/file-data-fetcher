# src/handlers/file_handlers/excel_fetch_handler.py

import pandas as pd
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler

class ExcelFetchHandler(BaseFileFetchHandler):
    """
    Handler class for processing Excel files.

    :param processed_registry: The registry used to track processed files.
    :type processed_registry: ProcessedFilesRegistry
    """

    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Read an Excel file and return a pandas DataFrame.

        :param file_path: The path to the Excel file.
        :type file_path: str
        :return: A pandas DataFrame containing the content of the Excel file.
        :rtype: pd.DataFrame
        """
        return pd.read_excel(file_path)
