"""
Logging Utilities

Provides sanitized logging functions to prevent exposure of sensitive information.
"""
import uuid
import logging
import traceback
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Patterns to detect and redact sensitive information
SENSITIVE_PATTERNS = [
    (r'password["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', '[PASSWORD_REDACTED]'),
    (r'token["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', '[TOKEN_REDACTED]'),
    (r'api[_-]?key["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', '[API_KEY_REDACTED]'),
    (r'secret["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', '[SECRET_REDACTED]'),
    (r'authorization["\']?\s*[:=]\s*["\']?([^"\'&\s]+)', '[AUTH_REDACTED]'),
    (r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', '[CARD_REDACTED]'),  # Credit cards
    (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]'),  # SSN
]


def sanitize_string(text: str) -> str:
    """
    Sanitize string by removing sensitive information

    Args:
        text: String to sanitize

    Returns:
        Sanitized string with sensitive info redacted
    """
    if not isinstance(text, str):
        return str(text)

    sanitized = text

    for pattern, replacement in SENSITIVE_PATTERNS:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    return sanitized


def get_error_id() -> str:
    """
    Generate unique error ID for tracking

    Returns:
        8-character error ID
    """
    return str(uuid.uuid4())[:8]


def log_error_sanitized(
    logger_instance: logging.Logger,
    message: str,
    exception: Optional[Exception] = None,
    context: Optional[dict] = None,
    include_trace: bool = False
) -> str:
    """
    Log error with sanitization and unique error ID

    Args:
        logger_instance: Logger to use
        message: Error message
        exception: Optional exception object
        context: Optional context dictionary
        include_trace: Whether to include full stack trace (DEBUG only)

    Returns:
        Error ID for tracking
    """
    error_id = get_error_id()

    # Sanitize message
    sanitized_message = sanitize_string(message)

    # Log basic error with ID
    if exception:
        exception_type = type(exception).__name__
        logger_instance.error(
            f"[{error_id}] {sanitized_message}: {exception_type}"
        )
    else:
        logger_instance.error(f"[{error_id}] {sanitized_message}")

    # Log context if provided (sanitized)
    if context:
        sanitized_context = {
            k: sanitize_string(str(v)) for k, v in context.items()
        }
        logger_instance.debug(f"[{error_id}] Context: {sanitized_context}")

    # Log full trace only in DEBUG mode
    if include_trace or logger_instance.isEnabledFor(logging.DEBUG):
        if exception:
            trace = traceback.format_exc()
            sanitized_trace = sanitize_string(trace)
            logger_instance.debug(f"[{error_id}] Full trace:\n{sanitized_trace}")

    return error_id


def log_repository_error(
    logger_instance: logging.Logger,
    operation: str,
    model_name: str,
    record_id: Optional[int],
    exception: Exception
) -> str:
    """
    Log repository error with standardized format

    Args:
        logger_instance: Logger to use
        operation: Operation being performed (find, save, update, delete)
        model_name: Model class name
        record_id: Record ID (if applicable)
        exception: Exception that occurred

    Returns:
        Error ID for tracking
    """
    if record_id:
        message = f"Error {operation} {model_name} with id {record_id}"
    else:
        message = f"Error {operation} {model_name}"

    context = {
        "operation": operation,
        "model": model_name,
        "record_id": record_id,
    }

    return log_error_sanitized(
        logger_instance,
        message,
        exception=exception,
        context=context
    )


def create_user_safe_error(error_id: str, operation: str = "operation") -> dict:
    """
    Create user-safe error response without exposing internals

    Args:
        error_id: Error ID for tracking
        operation: Operation that failed

    Returns:
        User-safe error dictionary
    """
    return {
        "error": f"An error occurred during {operation}",
        "error_id": error_id,
        "message": "Please contact support with this error ID if the problem persists"
    }


class SanitizedLogger:
    """
    Wrapper around logging.Logger that automatically sanitizes all messages
    """

    def __init__(self, logger_instance: logging.Logger):
        self._logger = logger_instance

    def debug(self, message: str, *args, **kwargs):
        """Log debug message (sanitized)"""
        self._logger.debug(sanitize_string(message), *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        """Log info message (sanitized)"""
        self._logger.info(sanitize_string(message), *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        """Log warning message (sanitized)"""
        self._logger.warning(sanitize_string(message), *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        """Log error message (sanitized)"""
        self._logger.error(sanitize_string(message), *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        """Log critical message (sanitized)"""
        self._logger.critical(sanitize_string(message), *args, **kwargs)


def get_sanitized_logger(name: str) -> SanitizedLogger:
    """
    Get a sanitized logger instance

    Args:
        name: Logger name (usually __name__)

    Returns:
        SanitizedLogger instance
    """
    return SanitizedLogger(logging.getLogger(name))