# src/handlers/files_monitor.py

import os
import time
import pandas as pd
import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from pandas import DataFrame
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from src.utils.logger import setup_logger
from src.handlers.db_handlers.base_db_handler import BaseDBHandler
from typing import Dict, Callable


class FileMonitorHandler(FileSystemEventHandler):
    """
    FileMonitorHandler handles the detection of new files and initiates
    processing for supported file types (CSV and Excel). Uses an asynchronous
    approach to allow concurrent file handling.

    :param db_handler: The database handler to be used for storing processed data.
    :type db_handler: BaseDBHandler
    :param dry_run: If True, no data will be inserted into the database (for testing purposes).
    :type dry_run: bool
    :param file_handlers: A dictionary mapping file extensions to file handlers.
    :type file_handlers: Dict[str, Callable]
    :param loop: The asyncio event loop to run coroutines.
    """

    def __init__(self, db_handler: 'BaseDBHandler', dry_run: bool = False, file_handlers: Dict[str, Callable] = None, loop=None) -> None:
        """
        Initializes the FileMonitorHandler.

        :param db_handler: The database handler used for saving processed data.
        :param dry_run: If set to True, only logs operations without committing data to the database.
        :param file_handlers: A dictionary mapping file extensions to handler classes.
        :param loop: The asyncio event loop to run coroutines.
        """
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run
        self.file_handlers = file_handlers or {
            ".csv": CSVFetchHandler,
            ".xls": ExcelFetchHandler,
            ".xlsx": ExcelFetchHandler,
        }

        # Explicitly set a new event loop for the thread if one is not provided
        self.loop = loop or asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)  # Set the event loop for this thread

    def is_file_ready(self, file_path: str) -> bool:
        """
        Checks if a file is ready for processing by comparing its size over a short time interval.

        :param file_path: Path to the file being checked.
        :type file_path: str
        :return: True if the file size is stable, indicating the file is ready for processing.
        :rtype: bool
        """
        try:
            initial_size = os.path.getsize(file_path)
            time.sleep(2)
            new_size = os.path.getsize(file_path)
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking file readiness: {e}")
            return False

    async def handle_file(self, file_path: str):
        """
        Handles file processing asynchronously, assigning the appropriate handler based on file extension.

        :param file_path: The path to the newly created file.
        """
        ext = os.path.splitext(file_path)[-1]
        handler_class = self.file_handlers.get(ext)

        self.logger.info(f"Attempting to handle the file {file_path}")

        if handler_class is None:
            self.logger.warning(f"Unsupported file type: {file_path}")
            return

        # Log file content if it's a CSV file
        if ext == ".csv":
            try:
                self.logger.info(f"Reading content of CSV file: {file_path}")
                df = pd.read_csv(file_path)  # Read the CSV file
                self.logger.info(f"File content:\n{df.to_string(index=False)}")  # Print the content of the CSV file
            except Exception as e:
                self.logger.error(f"Error reading CSV file {file_path}: {e}")

        handler = handler_class()
        processed_data = handler.process_file(file_path)

        if processed_data is not None:
            records, filename, checksum = processed_data
            if self.dry_run:
                self.logger.info(f"Dry run: Validated file {file_path}. No data inserted.")
            else:
                # Convert the list of dicts into a DataFrame before saving
                df = pd.DataFrame(records)
                self.db_handler.save_data(df)

    def on_created(self, event: 'FileSystemEvent') -> None:
        """
        Event handler triggered when a new file is created in the monitored directory.

        :param event: The file system event object containing details about the created file.
        """
        self.logger.info(f"File event detected: {event.src_path}")

        if event.is_directory:
            return

        file_path = event.src_path

        if not self.is_file_ready(file_path):
            self.logger.info(f"File {file_path} is not ready. Will retry later.")
            return

        # Use run_coroutine_threadsafe to ensure file handling runs in the correct event loop
        self.logger.info(f"File {file_path} is ready. Processing it...")
        asyncio.run_coroutine_threadsafe(self.handle_file(file_path), self.loop)


class FolderMonitor:
    """
    FolderMonitor manages the monitoring of a specific directory for new files
    and delegates the file processing to `FileMonitorHandler`.

    This class utilizes `watchdog.Observer` to watch for file changes in the
    specified folder and processes files as they appear, using appropriate file
    handlers for each file type.

    :param folder_to_monitor: The directory path to monitor for new files.
    :type folder_to_monitor: str
    :param poll_interval: The time interval (in seconds) to check for changes.
    :type poll_interval: int
    :param db_handler: The database handler responsible for saving the processed files.
    :type db_handler: BaseDBHandler
    :param dry_run: If True, processes files without inserting data into the database (for testing).
    :type dry_run: bool
    :param file_handlers: A dictionary mapping file extensions to handler classes.
    :type file_handlers: Dict[str, Callable]
    """

    def __init__(self, folder_to_monitor: str, poll_interval: int, db_handler: 'BaseDBHandler',
                 dry_run: bool = False, file_handlers: Dict[str, Callable] = None) -> None:
        """
        Initializes the FolderMonitor with folder monitoring settings.

        :param folder_to_monitor: The folder path to monitor for file changes.
        :param poll_interval: The time interval (in seconds) between folder checks.
        :param db_handler: Database handler to store the processed file data.
        :param dry_run: If set to True, processes files but skips inserting data into the database.
        :param file_handlers: A dictionary mapping file extensions to handler classes.
        """
        self.folder_to_monitor = folder_to_monitor
        self.poll_interval = poll_interval
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run
        self.file_handlers = file_handlers

    def start_monitoring(self) -> None:
        """
        Starts the folder monitoring process by setting up a `watchdog.Observer`.

        Continuously monitors the folder at the specified interval for file creation events
        and delegates file processing to the `FileMonitorHandler`. Stops monitoring if a
        `KeyboardInterrupt` is raised.
        """
        self.logger.info(f"Monitoring folder: {self.folder_to_monitor}")
        event_handler = FileMonitorHandler(self.db_handler, dry_run=self.dry_run, file_handlers=self.file_handlers)
        observer = Observer()
        observer.schedule(event_handler, self.folder_to_monitor, recursive=False)
        observer.start()

        self.logger.info(f"Observer started. Waiting for file events in {self.folder_to_monitor}...")

        try:
            while True:
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.logger.info("Shutting down folder monitoring...")
            observer.stop()
        observer.join()
