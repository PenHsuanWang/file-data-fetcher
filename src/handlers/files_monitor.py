# src/handlers/folder_monitor.py

import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelHandler
from db_handlers.mongo_handler import MongoDBHandler
from db_handlers.postgres_handler import PostgresHandler
from db_handlers.snowflake_handler import SnowflakeHandler
from logger import Logger

class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.logger = Logger().get_logger()

    def on_created(self, event):
        if event.is_directory:
            return

        # Detect file type and process accordingly
        file_path = event.src_path
        if file_path.endswith(".csv"):
            handler = CSVHandler(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            handler = ExcelHandler(file_path)
        else:
            self.logger.warning(f"Unsupported file type: {file_path}")
            return

        df = handler.process_file()
        if df is not None:
            self.db_handler.save_data(df)

class FolderMonitor:
    def __init__(self, folder_to_monitor, poll_interval, db_handler):
        self.folder_to_monitor = folder_to_monitor
        self.poll_interval = poll_interval
        self.db_handler = db_handler
        self.logger = Logger().get_logger()

    def start_monitoring(self):
        self.logger.info(f"Monitoring folder: {self.folder_to_monitor}")
        event_handler = FileMonitorHandler(self.db_handler)
        observer = Observer()
        observer.schedule(event_handler, self.folder_to_monitor, recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
