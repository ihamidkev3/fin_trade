"""
Logging utility for fin_trade pipeline.
Provides centralized logging configuration and utility functions.
"""

import logging
import logging.handlers
import os
from datetime import datetime
import pandas as pd
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Logging configuration
LOGGER_CONFIG = {
    'LOG_DIR': os.path.join(BASE_DIR, 'logs'),
    'LOG_FILE': 'fin_trade_pipeline.log',
    'MAX_BYTES': 10485760,  # 10MB
    'BACKUP_COUNT': 5,
    'LOG_FORMAT': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'DEFAULT_LEVEL': 'DEBUG'
}

class Logger:
    def __init__(self, log_file="logs/fin_trade_pipeline.log", log_level=logging.DEBUG):
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.logger = logging.getLogger("fin_trade_pipeline")
        self.logger.setLevel(log_level)

        # Rotating File handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def debug(self, message):
        """Log debug level messages for detailed debugging information."""
        self.logger.debug(message)

    def info(self, message):
        """Log info level messages for general process information."""
        self.logger.info(message)

    def warning(self, message):
        """Log warning level messages for concerning but non-critical issues."""
        self.logger.warning(message)

    def error(self, message):
        """Log error level messages for issues that prevent normal operation."""
        self.logger.error(message)
        
    def critical(self, message):
        """Log critical level messages for severe errors that require immediate attention."""
        self.logger.critical(message)

# Create default logger instance with DEBUG level
LOGGER = Logger() 