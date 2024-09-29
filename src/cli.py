# src/cli.py

import os
import argparse
from dotenv import load_dotenv
from src.handlers.files_monitor import FolderMonitor
from src.handlers.db_handlers.mongo_handler import MongoDBHandler
from src.handlers.db_handlers.postgres_handler import PostgresHandler
from src.handlers.db_handlers.snowflake_handler import SnowflakeHandler
from src.utils.logger import Logger

# Set up argument parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="CLI Tool for Monitoring Files and Saving to Databases")

    parser.add_argument('--source', required=True, help="Folder path to monitor")
    parser.add_argument('--poll-interval', type=int, default=5, help="Polling interval in seconds")
    parser.add_argument('--db', required=True, choices=['mongodb', 'postgresql', 'snowflake'], help="Database choice")
    parser.add_argument('--env-file', default='.env', help="Path to .env file for configuration")

    return parser.parse_args()

# Main function for setting up and running the folder monitor
def main():
    # Parse CLI arguments
    args = parse_arguments()

    # Load environment variables from the specified .env file
    load_dotenv(args.env_file)

    logger = Logger().get_logger()

    # Set folder to monitor and poll interval
    folder_to_monitor = args.source
    poll_interval = args.poll_interval

    # Select the appropriate database handler based on CLI input
    db_choice = args.db
    if db_choice == 'mongodb':
        db_handler = MongoDBHandler()
    elif db_choice == 'postgresql':
        db_handler = PostgresHandler()
    elif db_choice == 'snowflake':
        db_handler = SnowflakeHandler()
    else:
        logger.error(f"Unsupported database choice: {db_choice}")
        exit(1)

    logger.info(f"Monitoring folder: {folder_to_monitor} with polling interval {poll_interval} seconds.")
    logger.info(f"Saving data to {db_choice} database.")

    # Start the folder monitoring process
    folder_monitor = FolderMonitor(folder_to_monitor, poll_interval, db_handler)
    folder_monitor.start_monitoring()

if __name__ == "__main__":
    main()
