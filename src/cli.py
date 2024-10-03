# src/cli.py

import os
import argparse
from dotenv import load_dotenv
from src.handlers.files_monitor import FolderMonitor
from src.handlers.db_handlers.mongo_handler import MongoDBHandler
from src.handlers.db_handlers.postgres_handler import PostgresHandler
from src.handlers.db_handlers.snowflake_handler import SnowflakeHandler
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from src.utils.logger import setup_logger


# Set up argument parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="CLI Tool for Monitoring Files and Saving to Databases")

    parser.add_argument('--source', required=True, help="Folder path to monitor")
    parser.add_argument('--poll-interval', type=int, default=5, help="Polling interval in seconds")
    parser.add_argument('--db', required=True, choices=['mongodb', 'postgresql', 'snowflake'], help="Database choice")
    parser.add_argument('--env-file', default='.env', help="Path to .env file for configuration")
    parser.add_argument('--dry-run', action='store_true', help="Dry-run mode, only validate files without inserting data")
    parser.add_argument('--file-types', nargs='+', choices=['csv', 'excel'], help="File types to monitor (e.g. csv, excel)")

    return parser.parse_args()


# Main function for setting up and running the folder monitor
def main():
    # Parse CLI arguments
    args = parse_arguments()

    # Load environment variables from the specified .env file
    load_dotenv(args.env_file)

    # Setup logger (log before folder validation to catch early issues)
    logger = setup_logger()

    # Validate folder path
    folder_to_monitor = args.source
    if not os.path.exists(folder_to_monitor):
        logger.error(f"Folder {folder_to_monitor} does not exist.")
        exit(1)

    poll_interval = args.poll_interval

    # Select the appropriate database handler based on CLI input
    db_choice = args.db
    db_handler = None
    if db_choice == 'mongodb':
        db_handler = MongoDBHandler()
    elif db_choice == 'postgresql':
        db_handler = PostgresHandler()
    elif db_choice == 'snowflake':
        db_handler = SnowflakeHandler()

    # File handlers - allow CLI to specify which file types to monitor (new feature)
    file_handlers = {}
    if args.file_types:
        if 'csv' in args.file_types:
            file_handlers['.csv'] = CSVFetchHandler
        if 'excel' in args.file_types:
            file_handlers['.xls'] = ExcelFetchHandler
            file_handlers['.xlsx'] = ExcelFetchHandler
    else:
        # Default to CSV and Excel if no specific file types are provided
        file_handlers = {
            '.csv': CSVFetchHandler,
            '.xls': ExcelFetchHandler,
            '.xlsx': ExcelFetchHandler
        }

    logger.info(f"Monitoring folder: {folder_to_monitor} with polling interval {poll_interval} seconds.")
    logger.info(f"Saving data to {db_choice} database.")
    logger.info(f"File types being monitored: {', '.join(file_handlers.keys())}")

    # Start the folder monitoring process with dynamic file handlers
    folder_monitor = FolderMonitor(folder_to_monitor, poll_interval, db_handler, dry_run=args.dry_run, file_handlers=file_handlers)
    folder_monitor.start_monitoring()

if __name__ == "__main__":
    main()