"""
Application Constants

Centralized configuration constants for the entire application.
Avoids magic numbers and provides single source of truth.
"""
import os


class PaginationConfig:
    """Pagination-related constants"""
    DEFAULT_SKIP = 0
    DEFAULT_LIMIT = 100
    MAX_LIMIT = int(os.getenv('PAGINATION_MAX_LIMIT', '1000'))
    MIN_LIMIT = 1


class CacheConfig:
    """Cache TTL and configuration constants"""
    # Default TTLs in seconds
    DEFAULT_TTL = 300  # 5 minutes
    PRODUCT_LIST_TTL = 300  # 5 minutes
    PRODUCT_ITEM_TTL = 300  # 5 minutes
    CATEGORY_LIST_TTL = 3600  # 1 hour (rarely changes)
    CATEGORY_ITEM_TTL = 3600  # 1 hour


class LogConfig:
    """Logging configuration constants"""
    MAX_LOG_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB
    LOG_BACKUP_COUNT = 5
    DEFAULT_LOG_LEVEL = 'INFO'


class RateLimitConfig:
    """Rate limiting constants"""
    GLOBAL_CALLS_PER_PERIOD = 100
    GLOBAL_PERIOD_SECONDS = 60

    # Endpoint-specific limits
    ORDER_CREATE_CALLS = 10  # requests per minute
    ORDER_CREATE_PERIOD = 60  # seconds

    CLIENT_CREATE_CALLS = 5  # requests per minute
    CLIENT_CREATE_PERIOD = 60

    REVIEW_CREATE_CALLS = 3  # requests per minute
    REVIEW_CREATE_PERIOD = 60


class DatabaseConfig:
    """Database connection constants"""
    DEFAULT_POOL_SIZE = 50
    DEFAULT_MAX_OVERFLOW = 100
    DEFAULT_POOL_TIMEOUT = 10  # seconds (fail fast for high concurrency)
    DEFAULT_POOL_RECYCLE = 3600  # 1 hour


class ValidationConfig:
    """Validation-related constants"""
    # Price validation
    MIN_PRICE = 0.01  # Minimum price (must be positive)
    MAX_PRICE = 999999.99  # Maximum reasonable price

    # Stock validation
    MIN_STOCK = 0  # Minimum stock (non-negative)
    MAX_STOCK = 999999  # Maximum reasonable stock

    # String length limits
    MIN_NAME_LENGTH = 1
    MAX_NAME_LENGTH = 200
    MAX_DESCRIPTION_LENGTH = 2000
    MAX_EMAIL_LENGTH = 255

    # Phone validation
    PHONE_REGEX = r'^\+?[1-9]\d{6,19}$'  # International format

    # Price comparison precision
    PRICE_EPSILON = 0.01  # For float comparison


class ErrorMessages:
    """Centralized error message templates"""
    INSTANCE_NOT_FOUND = "{resource} with ID {id} not found"
    INSUFFICIENT_STOCK = "Insufficient stock for product {product_id}: requested {requested}, available {available}"
    PRICE_MISMATCH = "Price mismatch for product {product_id}: expected {expected}, got {actual}"
    INVALID_PAGINATION = "Invalid pagination: skip must be >= 0 and limit must be between {min} and {max}"
    PROTECTED_FIELD = "Cannot update protected field: {field}"
    INVALID_FIELD = "Invalid field for {model}: {field}"
    RATE_LIMIT_EXCEEDED = "Rate limit exceeded. Maximum {limit} requests per {period} seconds"