#!/usr/bin/env python3
"""
File Monitor using PyFilesystem2 (fs)
--------------------------------------
This module polls a given directory (using an FS object from PyFilesystem2) to detect new or modified files.
It then checks if the file is ready (its size is stable over a short interval) and processes it based on its extension.
Supported file types include CSV and Excel.
"""

import time
import logging
import pandas as pd
from fs import open_fs, errors

# Set up basic logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FileMonitorPyFS:
    def __init__(self, folder_to_monitor, poll_interval=5, dry_run=False, file_handlers=None):
        """
        :param folder_to_monitor: Directory path to monitor (local NAS mount or other FS URL, e.g., "osfs:///mnt/nas_share")
        :param poll_interval: Polling interval in seconds.
        :param dry_run: If True, files are processed but no downstream actions are taken.
        :param file_handlers: Optional dict mapping file extensions to handler functions.
        """
        # Open the filesystem; for local folders use "osfs:///<folder>"
        self.fs = open_fs(f'osfs://{folder_to_monitor}')
        self.folder = folder_to_monitor  # For logging purposes.
        self.poll_interval = poll_interval
        self.dry_run = dry_run
        self.logger = logger

        # Map file extensions to processing functions
        self.file_handlers = file_handlers or {
            ".csv": self.process_csv,
            ".xls": self.process_excel,
            ".xlsx": self.process_excel,
        }
        # Dictionary to track state: mapping filepath -> (size, modified_time)
        self.files_state = {}

    def scan_directory(self):
        """
        Scans the monitored directory and returns a dict of file paths with their size and modification time.
        """
        current_files = {}
        try:
            # Use fs.listdir to get a list of files (non-recursive in this example)
            for path in self.fs.listdir('/'):
                try:
                    info = self.fs.getinfo(path, namespaces=["details"])
                    # info.details is a dict with 'size' and 'modified'
                    current_files[path] = (info.details.get("size"), info.details.get("modified"))
                except Exception as e:
                    self.logger.error(f"Error reading info for {path}: {e}")
        except errors.CreateFailed as e:
            self.logger.error(f"Error scanning directory {self.folder}: {e}")
        return current_files

    def is_file_ready(self, file_path):
        """
        Checks if a file is ready for processing by verifying its size is stable over 2 seconds.
        """
        try:
            info_initial = self.fs.getinfo(file_path, namespaces=["details"])
            initial_size = info_initial.details.get("size")
            time.sleep(2)
            info_new = self.fs.getinfo(file_path, namespaces=["details"])
            new_size = info_new.details.get("size")
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking readiness for {file_path}: {e}")
            return False

    def process_csv(self, file_path):
        """
        Reads CSV content from the file using pandas.
        """
        try:
            self.logger.info(f"Processing CSV file: {file_path}")
            with self.fs.open(file_path, 'r') as f:
                df = pd.read_csv(f)
            self.logger.info(f"CSV content from {file_path}:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            self.logger.error(f"Error processing CSV file {file_path}: {e}")
            return None

    def process_excel(self, file_path):
        """
        Reads Excel content from the file using pandas.
        """
        try:
            self.logger.info(f"Processing Excel file: {file_path}")
            with self.fs.open(file_path, 'rb') as f:
                df = pd.read_excel(f)
            self.logger.info(f"Excel content from {file_path}:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {e}")
            return None

    def handle_file(self, file_path):
        ext = file_path.lower().rsplit('.', 1)[-1]
        ext = f".{ext}"
        handler = self.file_handlers.get(ext)
        if not handler:
            self.logger.warning(f"No handler for file type {ext}: {file_path}")
            return
        self.logger.info(f"Handling file: {file_path}")
        df = handler(file_path)
        if df is not None:
            if self.dry_run:
                self.logger.info(f"Dry run: Processed file {file_path} (no further actions).")
            else:
                # Placeholder for further processing (e.g. storing in a database)
                self.logger.info(f"File {file_path} processed successfully. Data shape: {df.shape}")

    def monitor(self):
        self.logger.info(f"Starting file monitoring on {self.folder}")
        self.files_state = self.scan_directory()
        while True:
            try:
                current_files = self.scan_directory()
                for file_path, stats in current_files.items():
                    # New or updated file detected
                    if file_path not in self.files_state or self.files_state[file_path] != stats:
                        self.logger.info(f"Detected new/modified file: {file_path}")
                        if self.is_file_ready(file_path):
                            self.handle_file(file_path)
                        else:
                            self.logger.info(f"File {file_path} is not ready for processing.")
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
        description="File Monitor using PyFilesystem2 (fs).")
    parser.add_argument("folder", help="Folder to monitor (absolute path).")
    parser.add_argument("--poll", type=int, default=5,
                        help="Polling interval in seconds (default: 5).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Enable dry run mode (simulate processing without further actions).")
    args = parser.parse_args()

    monitor = FileMonitorPyFS(args.folder, poll_interval=args.poll, dry_run=args.dry_run)
    monitor.monitor()