# src/main.py

import os
import time
import asyncio
import pandas as pd
from src.handlers.files_monitor import FolderMonitor
from src.handlers.db_handlers.mongo_handler import MongoDBHandler
from src.handlers.db_handlers.postgres_handler import PostgresHandler
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from src.utils.logger import setup_logger

# Define the folder to be monitored
TEST_FOLDER = "./test_monitor_folder"


def check_folder_exists():
    """
    Checks if the monitored folder exists.
    If the folder doesn't exist, it prints a message and exits the script.
    """
    if not os.path.exists(TEST_FOLDER):
        print(f"Folder {TEST_FOLDER} does not exist. Please run the file creation script to create it.")
        exit(1)


async def run_monitoring():
    """
    Function to run the file monitoring system, which monitors for CSV and Excel file creations.
    """
    # Initialize logger
    logger = setup_logger()

    # Select MongoDB as the database handler for demonstration purposes
    db_handler = MongoDBHandler()

    # Set up the file handlers for CSV and Excel files
    file_handlers = {
        '.csv': CSVFetchHandler,
        '.xls': ExcelFetchHandler,
        '.xlsx': ExcelFetchHandler
    }

    # Get the event loop
    loop = asyncio.get_event_loop()

    # Initialize the FolderMonitor to monitor the test folder
    folder_monitor = FolderMonitor(
        folder_to_monitor=TEST_FOLDER,
        poll_interval=2,  # Polling interval in seconds
        db_handler=db_handler,
        dry_run=True,  # Dry-run mode for testing (no actual DB insertion)
        file_handlers=file_handlers
    )

    # Run folder monitoring in a separate thread
    await asyncio.to_thread(folder_monitor.start_monitoring)


def main():
    """
    The main entry point of the script. Sets up a test environment, demonstrates file creation,
    and starts the folder monitoring process.
    """
    # Step 1: Check if the test folder exists
    check_folder_exists()

    # Step 2: Start the asyncio event loop to monitor the test folder
    loop = asyncio.get_event_loop()
    try:
        # Start the folder monitoring coroutine
        loop.run_until_complete(run_monitoring())
    except KeyboardInterrupt:
        print("Monitoring stopped.")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
