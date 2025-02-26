#!/usr/bin/env python3
"""
File Monitor using fsspec
--------------------------
This module polls a given directory using fsspec’s filesystem interface to detect new or modified files.
It checks whether files are “ready” (by confirming a stable size over a short interval) and processes them based on extension.
Supported file types include CSV and Excel.
"""

import time
import logging
import pandas as pd
import fsspec

# Set up basic logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FileMonitorFsspec:
    def __init__(self, folder_to_monitor, poll_interval=5, dry_run=False, file_handlers=None):
        """
        :param folder_to_monitor: Directory path to monitor (for local FS, use a standard path)
        :param poll_interval: Polling interval in seconds.
        :param dry_run: If True, files are processed but no further actions are taken.
        :param file_handlers: Optional dict mapping file extensions to processing functions.
        """
        # Create a filesystem object for local files. For other protocols, adjust the fs type.
        self.fs = fsspec.filesystem("file")
        self.folder = folder_to_monitor
        self.poll_interval = poll_interval
        self.dry_run = dry_run
        self.logger = logger

        # Map file extensions to processing functions.
        self.file_handlers = file_handlers or {
            ".csv": self.process_csv,
            ".xls": self.process_excel,
            ".xlsx": self.process_excel,
        }
        # Dictionary to store state: mapping file path -> (size, modified time)
        self.files_state = {}

    def scan_directory(self):
        """
        Scans the monitored directory and returns a dict of file paths with size and modification time.
        """
        current_files = {}
        try:
            # fsspec ls with detail=True returns a list of dicts with file info.
            files = self.fs.ls(self.folder, detail=True)
            for info in files:
                if info.get("type") == "file":
                    path = info["name"]
                    size = info.get("size")
                    mtime = info.get("mtime")
                    current_files[path] = (size, mtime)
        except Exception as e:
            self.logger.error(f"Error scanning directory {self.folder}: {e}")
        return current_files

    def is_file_ready(self, file_path):
        """
        Checks if a file is ready by ensuring its size is stable over 2 seconds.
        """
        try:
            info_initial = self.fs.info(file_path)
            initial_size = info_initial.get("size")
            time.sleep(2)
            info_new = self.fs.info(file_path)
            new_size = info_new.get("size")
            return initial_size == new_size
        except Exception as e:
            self.logger.error(f"Error checking readiness for {file_path}: {e}")
            return False

    def process_csv(self, file_path):
        """
        Processes a CSV file using pandas.
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
        Processes an Excel file using pandas.
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
                # Placeholder for additional processing (e.g., saving to database)
                self.logger.info(f"File {file_path} processed successfully. Data shape: {df.shape}")

    def monitor(self):
        self.logger.info(f"Starting file monitoring on {self.folder}")
        self.files_state = self.scan_directory()
        while True:
            try:
                current_files = self.scan_directory()
                for file_path, stats in current_files.items():
                    # Detect new or modified files by comparing state
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
        description="File Monitor using fsspec.")
    parser.add_argument("folder", help="Folder to monitor (absolute path).")
    parser.add_argument("--poll", type=int, default=5,
                        help="Polling interval in seconds (default: 5).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Enable dry run mode (simulate processing without further actions).")
    args = parser.parse_args()

    monitor = FileMonitorFsspec(args.folder, poll_interval=args.poll, dry_run=args.dry_run)
    monitor.monitor()