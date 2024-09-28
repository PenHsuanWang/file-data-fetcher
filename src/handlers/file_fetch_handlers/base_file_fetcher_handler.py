from abc import ABC, abstractmethod


class BaseFileHandler(ABC):
    @abstractmethod
    def process_file(self, file_path):
        pass
    