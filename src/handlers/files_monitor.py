# src/handlers/files_monitor.py

import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from db_handlers.mongo_handler import MongoDBHandler
from db_handlers.postgres_handler import PostgresHandler
from db_handlers.snowflake_handler import SnowflakeHandler
from src.utils.logger import setup_logger


class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, db_handler, dry_run=False):
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run

    def is_file_ready(self, file_path):
        try:
            initial_size = os.path.getsize(file_path)
            time.sleep(2)
            new_size = os.path.getsize(file_path)
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking file readiness: {e}")
            return False

    def on_created(self, event):
        if event.is_directory:
            return

        # Detect file type and process accordingly
        file_path = event.src_path
        if not self.is_file_ready(file_path):
            self.logger.info(f"File {file_path} is not ready. Will retry later.")
            return

        if file_path.endswith(".csv"):
            handler = CSVFetchHandler(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            handler = ExcelFetchHandler(file_path)
        else:
            self.logger.warning(f"Unsupported file type: {file_path}")
            return

        df = handler.process_file()
        if df is not None:
            if self.dry_run:
                self.logger.info(f"Dry run: Validated file {file_path}. No data inserted.")
            else:
                self.db_handler.save_data(df)


class FolderMonitor:
    def __init__(self, folder_to_monitor, poll_interval, db_handler, dry_run=False):
        self.folder_to_monitor = folder_to_monitor
        self.poll_interval = poll_interval
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run

    def start_monitoring(self):
        self.logger.info(f"Monitoring folder: {self.folder_to_monitor}")
        event_handler = FileMonitorHandler(self.db_handler, dry_run=self.dry_run)
        observer = Observer()
        observer.schedule(event_handler, self.folder_to_monitor, recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
