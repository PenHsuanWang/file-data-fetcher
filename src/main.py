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

# Example test folder (should exist or be created beforehand)
TEST_FOLDER = "./test_monitor_folder"


def setup_test_environment():
    """
    Sets up the test environment by creating a test folder and sample CSV/Excel files.
    This function simulates the creation of test files that will be monitored.
    """
    os.makedirs(TEST_FOLDER, exist_ok=True)

    # Create a sample CSV file
    csv_data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "San Francisco", "Los Angeles"]
    }
    df_csv = pd.DataFrame(csv_data)
    csv_file_path = os.path.join(TEST_FOLDER, "sample.csv")
    df_csv.to_csv(csv_file_path, index=False)

    # Create a sample Excel file
    excel_data = {
        "product": ["A", "B", "C"],
        "price": [100, 200, 300],
        "quantity": [10, 5, 2]
    }
    df_excel = pd.DataFrame(excel_data)
    excel_file_path = os.path.join(TEST_FOLDER, "sample.xlsx")
    df_excel.to_excel(excel_file_path, index=False)

    print(f"Sample CSV file created at: {csv_file_path}")
    print(f"Sample Excel file created at: {excel_file_path}")


async def run_monitoring():
    """
    Function to run the file monitoring example. This function demonstrates how to monitor
    a folder for CSV and Excel file creation and handle them asynchronously.
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

    # Initialize the FolderMonitor to monitor the test folder
    folder_monitor = FolderMonitor(
        folder_to_monitor=TEST_FOLDER,
        poll_interval=2,  # Polling interval in seconds
        db_handler=db_handler,
        dry_run=True,  # Dry-run mode for testing (no actual DB insertion)
        file_handlers=file_handlers
    )

    # Start monitoring the folder asynchronously
    await asyncio.to_thread(folder_monitor.start_monitoring)


def main():
    """
    The main entry point of the script. Sets up a test environment, demonstrates file creation,
    and starts the folder monitoring process.
    """
    # Step 1: Setup the test folder and create example CSV and Excel files
    setup_test_environment()

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
