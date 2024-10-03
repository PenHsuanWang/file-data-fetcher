# src/utils/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger():
    """
    Sets up a centralized logger for the application.
    Ensures logging only happens once for the entire app.
    """
    logger = logging.getLogger("app_logger")

    # Set log level from environment variable or default to DEBUG
    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logger.setLevel(getattr(logging, log_level, logging.DEBUG))

    # Prevent multiple handlers if the logger is already configured
    if not logger.handlers:
        # Create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Create file handler for logging to a file
        log_file = os.getenv("LOG_FILE", "application.log")
        fh = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
        fh.setLevel(logging.ERROR)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger