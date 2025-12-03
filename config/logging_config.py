"""
Centralized Logging Configuration

Provides consistent logging configuration across the entire application.
"""
import os
import logging
import logging.config
from pathlib import Path


# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'default',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': 'logs/app.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'encoding': 'utf-8',
        },
        'error_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'detailed',
            'filename': 'logs/error.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'encoding': 'utf-8',
        },
    },
    'root': {
        'level': os.getenv('LOG_LEVEL', 'INFO'),
        'handlers': ['console', 'file', 'error_file'],
    },
    'loggers': {
        'uvicorn': {
            'level': 'INFO',
            'handlers': ['console'],
            'propagate': False,
        },
        'uvicorn.error': {
            'level': 'INFO',
            'handlers': ['console', 'error_file'],
            'propagate': False,
        },
        'uvicorn.access': {
            'level': 'WARNING',  # Reduce access logs noise
            'handlers': ['console'],
            'propagate': False,
        },
        'sqlalchemy.engine': {
            'level': 'WARNING',  # Only log SQL warnings/errors
            'handlers': ['console', 'file'],
            'propagate': False,
        },
    },
}


def setup_logging():
    """
    Setup centralized logging configuration

    Call this function once at application startup in main.py
    """
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)
    logger.info("✅ Logging configured successfully")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)
