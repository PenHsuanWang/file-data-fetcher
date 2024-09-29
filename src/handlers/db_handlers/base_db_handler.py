# src/handlers/db_handlers/base_db_handler.py

from abc import ABC, abstractmethod
from src.utils.logger import setup_logger

class BaseDBHandler(ABC):
    def __init__(self):
        # Initialize the unified logger for all DB handlers
        self.logger = setup_logger()

    @abstractmethod
    def insert_records(self, records):
        """
        Abstract method to be implemented by specific DB handlers.
        Responsible for inserting records into the respective DB.
        """
        pass
