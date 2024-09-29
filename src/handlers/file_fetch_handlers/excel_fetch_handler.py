# handlers/file_handlers/excel_fetch_handler.py

import pandas as pd
import hashlib
import time
import os
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler
from models.data_record import DataRecord
from utils.logger import logger

class ExcelFetchHandler(BaseFileFetchHandler):
    def __init__(self, processed_registry):
        self.processed_registry = processed_registry

    def is_file_ready(self, file_path):
        """Check if the file is ready for processing."""
        try:
            initial_size = os.path.getsize(file_path)
            time.sleep(1)
            new_size = os.path.getsize(file_path)
            return initial_size == new_size
        except Exception as e:
            logger.error(f"Error checking file readiness: {e}")
            return False

    def calculate_checksum(self, file_path):
        """Calculate MD5 checksum of the file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum for {file_path}: {e}")
            return None

    def process_file(self, file_path):
        filename = os.path.basename(file_path)
        if self.processed_registry.is_processed(filename):
            logger.info(f"File {filename} already processed.")
            return

        if not self.is_file_ready(file_path):
            logger.info(f"File {filename} is not ready. Will retry later.")
            return

        checksum = self.calculate_checksum(file_path)
        if not checksum:
            logger.error(f"Failed to calculate checksum for {filename}.")
            return

        try:
            df = pd.read_excel(file_path)
            records = df.to_dict(orient='records')
            validated_records = []
            for record in records:
                try:
                    validated_record = DataRecord(**record).dict()
                    validated_records.append(validated_record)
                except ValidationError as e:
                    logger.error(f"Validation error for record {record}: {e}")
            if validated_records:
                return validated_records, filename, checksum
            else:
                logger.info(f"No valid records found in {filename}.")
        except Exception as e:
            logger.error(f"Failed to process Excel file {file_path}: {e}")
