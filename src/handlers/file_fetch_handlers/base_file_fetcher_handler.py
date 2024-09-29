# handlers/file_handlers/base_file_fetch_handler.py

from abc import ABC, abstractmethod

class BaseFileFetchHandler(ABC):
    @abstractmethod
    def process_file(self, file_path):
        pass
