#!/usr/bin/env python3
import os
import time
import logging
from pathlib import Path
import pandas as pd

# Set up the logger
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FileMonitor:
    """
    Monitors a directory by polling for file changes.
    Uses os and pathlib to list and track files. When new or updated files are detected,
    the file is checked to ensure it is ready for processing (its size is stable)
    and then processed using an appropriate handler.
    """

    def __init__(self, folder_to_monitor, poll_interval=5, dry_run=False, file_handlers=None):
        """
        Initializes the FileMonitor.

        :param folder_to_monitor: Directory path to monitor.
        :param poll_interval: Time interval in seconds between polls.
        :param dry_run: If True, process files without taking further actions (e.g. database insertion).
        :param file_handlers: Dictionary mapping file extensions to processing functions.
        """
        self.folder_to_monitor = Path(folder_to_monitor)
        self.poll_interval = poll_interval
        self.dry_run = dry_run
        self.logger = logger
        # Map file extensions to handler functions
        self.file_handlers = file_handlers or {
            ".csv": self.process_csv,
            ".xls": self.process_excel,
            ".xlsx": self.process_excel
        }
        # Maintain state of files (file path -> (size, modification time))
        self.files_state = {}

    def scan_directory(self):
        """
        Scans the monitored directory and returns a dictionary of file paths with their size and modification time.
        """
        current_files = {}
        try:
            for file_path in self.folder_to_monitor.iterdir():
                if file_path.is_file():
                    try:
                        stat = file_path.stat()
                        current_files[str(file_path)] = (stat.st_size, stat.st_mtime)
                    except Exception as e:
                        self.logger.error(f"Error reading file {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Error scanning directory {self.folder_to_monitor}: {e}")
        return current_files

    def is_file_ready(self, file_path):
        """
        Checks if a file is ready for processing by verifying its size is stable over 2 seconds.

        :param file_path: Path of the file to check.
        :return: True if the file size is stable; otherwise, False.
        """
        try:
            file_path_obj = Path(file_path)
            initial_size = file_path_obj.stat().st_size
            time.sleep(2)
            new_size = file_path_obj.stat().st_size
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking file readiness for {file_path}: {e}")
            return False

    def process_csv(self, file_path):
        """
        Processes a CSV file by reading and logging its content.

        :param file_path: CSV file path.
        :return: DataFrame containing CSV data, or None if error.
        """
        try:
            self.logger.info(f"Processing CSV file: {file_path}")
            df = pd.read_csv(file_path)
            self.logger.info(f"CSV content from {file_path}:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            self.logger.error(f"Error processing CSV file {file_path}: {e}")
            return None

    def process_excel(self, file_path):
        """
        Processes an Excel file by reading and logging its content.

        :param file_path: Excel file path.
        :return: DataFrame containing Excel data, or None if error.
        """
        try:
            self.logger.info(f"Processing Excel file: {file_path}")
            df = pd.read_excel(file_path)
            self.logger.info(f"Excel content from {file_path}:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {e}")
            return None

    def handle_file(self, file_path):
        """
        Determines the appropriate file handler based on extension and processes the file.

        :param file_path: Path to the file to handle.
        """
        ext = Path(file_path).suffix.lower()
        handler = self.file_handlers.get(ext)
        if not handler:
            self.logger.warning(f"No handler for file type {ext}: {file_path}")
            return
        self.logger.info(f"Handling file: {file_path}")
        df = handler(file_path)
        if df is not None:
            if self.dry_run:
                self.logger.info(f"Dry run enabled: Processed file {file_path} (no further actions taken)")
            else:
                # Placeholder: Insert further processing or saving logic here
                self.logger.info(f"File {file_path} processed successfully. Data shape: {df.shape}")

    def monitor(self):
        """
        Continuously polls the directory, detects new or modified files,
        and processes them once they are ready.
        """
        self.logger.info(f"Starting file monitoring on {self.folder_to_monitor}")
        self.files_state = self.scan_directory()
        while True:
            try:
                current_files = self.scan_directory()
                # Detect new or updated files by comparing with previous state
                for file_path, stats in current_files.items():
                    # New file or modified file (if size or modification time has changed)
                    if file_path not in self.files_state or self.files_state[file_path] != stats:
                        self.logger.info(f"Detected new or modified file: {file_path}")
                        if self.is_file_ready(file_path):
                            self.handle_file(file_path)
                        else:
                            self.logger.info(f"File {file_path} is not ready for processing.")
                # Update stored state
                self.files_state = current_files
                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                self.logger.info("Stopping file monitoring.")
                break
            except Exception as e:
                self.logger.error(f"Error during monitoring: {e}")
                time.sleep(self.poll_interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="File Monitor using os and pathlib (standard library)."
    )
    parser.add_argument("folder", help="Folder to monitor")
    parser.add_argument("--poll", type=int, default=5,
                        help="Polling interval in seconds (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Enable dry run mode (process files without further actions)")
    args = parser.parse_args()

    monitor = FileMonitor(args.folder, poll_interval=args.poll, dry_run=args.dry_run)
    monitor.monitor()
