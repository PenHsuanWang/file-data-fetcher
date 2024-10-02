# src/handlers/files_monitor.py

import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pandas import DataFrame
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from db_handlers.mongo_handler import MongoDBHandler
from db_handlers.postgres_handler import PostgresHandler
from db_handlers.snowflake_handler import SnowflakeHandler
from src.utils.logger import setup_logger


class FileMonitorHandler(FileSystemEventHandler):
    """
    FileMonitorHandler handles the detection of new files and initiates
    processing for supported file types (CSV and Excel).

    Inherits from `FileSystemEventHandler` to react to file creation events
    in a monitored directory. Based on the file extension, it delegates the
    file processing to the appropriate handler and stores the result in the
    associated database.

    :param db_handler: The database handler to be used for storing processed data.
    :type db_handler: BaseDBHandler
    :param dry_run: If True, no data will be inserted into the database (for testing purposes).
    :type dry_run: bool
    """

    def __init__(self, db_handler: 'BaseDBHandler', dry_run: bool = False) -> None:
        """
        Initializes the FileMonitorHandler.

        :param db_handler: The database handler used for saving processed data.
        :param dry_run: If set to True, only logs operations without committing data to the database.
        """
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run

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

    def on_created(self, event: 'FileSystemEvent') -> None:
        """
        Event handler triggered when a new file is created in the monitored directory.

        Determines the file type and delegates its processing to the appropriate file handler.
        If the file is not ready or unsupported, logs the issue.

        :param event: The file system event object containing details about the created file.
        :type event: FileSystemEvent
        """
        if event.is_directory:
            return

        file_path = event.src_path
        if not self.is_file_ready(file_path):
            self.logger.info(f"File {file_path} is not ready. Will retry later.")
            return

        # Process CSV files
        if file_path.endswith(".csv"):
            handler = CSVFetchHandler(file_path)
        # Process Excel files
        elif file_path.endswith((".xls", ".xlsx")):
            handler = ExcelFetchHandler(file_path)
        else:
            self.logger.warning(f"Unsupported file type: {file_path}")
            return

        # Process the file and insert into the database
        df = handler.process_file()
        if df is not None:
            if self.dry_run:
                self.logger.info(f"Dry run: Validated file {file_path}. No data inserted.")
            else:
                self.db_handler.save_data(df)


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
    """

    def __init__(self, folder_to_monitor: str, poll_interval: int, db_handler: 'BaseDBHandler',
                 dry_run: bool = False) -> None:
        """
        Initializes the FolderMonitor with folder monitoring settings.

        :param folder_to_monitor: The folder path to monitor for file changes.
        :param poll_interval: The time interval (in seconds) between folder checks.
        :param db_handler: Database handler to store the processed file data.
        :param dry_run: If set to True, processes files but skips inserting data into the database.
        """
        self.folder_to_monitor = folder_to_monitor
        self.poll_interval = poll_interval
        self.db_handler = db_handler
        self.logger = setup_logger()
        self.dry_run = dry_run

    def start_monitoring(self) -> None:
        """
        Starts the folder monitoring process by setting up a `watchdog.Observer`.

        Continuously monitors the folder at the specified interval for file creation events
        and delegates file processing to the `FileMonitorHandler`. Stops monitoring if a
        `KeyboardInterrupt` is raised.
        """
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

