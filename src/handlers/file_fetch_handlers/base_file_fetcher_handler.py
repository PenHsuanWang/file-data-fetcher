# handlers/file_handlers/base_file_fetch_handler.py

from abc import ABC, abstractmethod
import hashlib
import time
import os
from src.utils.logger import setup_logger
from pydantic import ValidationError
import pandas as pd
from typing import Optional, Tuple, List


class BaseFileFetchHandler(ABC):
    """
    Abstract base class for file fetch handlers.

    :param processed_registry: The registry used to track processed files.
    :type processed_registry: ProcessedFilesRegistry
    """

    def __init__(self, data_model) -> None:
        """
        Initialize the BaseFileFetchHandler with the processed registry and logger.

        :param processed_registry: An instance of the processed files registry.
        :type processed_registry: ProcessedFilesRegistry
        """
        self.data_model = data_model
        self.logger = setup_logger()  # Initialize the logger here

    def is_file_ready(self, file_path: str) -> bool:
        """
        Check if the file is stable and ready for processing by comparing its size after a delay.

        :param file_path: The path to the file.
        :type file_path: str
        :return: True if the file size has stabilized, False otherwise.
        :rtype: bool
        """
        try:
            initial_size = os.path.getsize(file_path)
            time.sleep(1)
            new_size = os.path.getsize(file_path)
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking file readiness: {e}")
            return False

    def calculate_checksum(self, file_path: str) -> Optional[str]:
        """
        Calculate the MD5 checksum of the file.

        :param file_path: The path to the file.
        :type file_path: str
        :return: The MD5 checksum string if successful, or None if the checksum calculation fails.
        :rtype: Optional[str]
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating checksum for {file_path}: {e}")
            return None

    @abstractmethod
    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Abstract method to read a file and return a pandas DataFrame.

        :param file_path: The path to the file.
        :type file_path: str
        :return: A pandas DataFrame containing the file's content.
        :rtype: pd.DataFrame
        """
        pass

    def process_file(self, file_path: str) -> Optional[Tuple[List[dict], str, str]]:
        """
        Process the file if it's ready, calculate checksum, validate data records, and return metadata.

        :param file_path: The path to the file.
        :type file_path: str
        :return: A tuple containing validated records, filename, and checksum if successful, or None if an error occurs.
        :rtype: Optional[Tuple[List[dict], str, str]]
        """
        filename = os.path.basename(file_path)

        if not self.is_file_ready(file_path):
            self.logger.info(f"File {filename} is not ready. Will retry later.")
            return

        checksum = self.calculate_checksum(file_path)
        if not checksum:
            self.logger.error(f"Failed to calculate checksum for {filename}.")
            return

        try:
            df = self.read_file(file_path)
            records = df.to_dict(orient='records')
            validated_records = []
            for record in records:
                try:
                    validated_record = self.data_model(**record).dict()
                    validated_records.append(validated_record)
                except ValidationError as e:
                    self.logger.error(f"Validation error for record {record}: {e}")
            if validated_records:
                return validated_records, filename, checksum
            else:
                self.logger.info(f"No valid records found in {filename}.")
        except Exception as e:
            self.logger.error(f"Failed to process file {file_path}: {e}")
