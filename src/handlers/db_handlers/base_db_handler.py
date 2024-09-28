from abc import ABC, abstractmethod


class BaseFileHandler(ABC):
    @abstractmethod
    def insert_records(self, records):
        pass

