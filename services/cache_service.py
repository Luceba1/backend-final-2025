"""
Cache Service Module

Provides high-level caching operations using Redis with automatic
serialization, TTL management, error handling, and distributed cache stampede protection.
"""
import json
import logging
import time
from typing import Optional, Any, List, Callable
from datetime import timedelta
import os

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CacheService:
    """
    Cache service for storing and retrieving data from Redis

    Handles JSON serialization/deserialization and provides
    convenient methods for common caching patterns.

    Uses distributed Redis locks for cache stampede protection,
    making it safe for multi-worker/multi-process deployments.
    """

    def __init__(self):
        self.redis_client = get_redis_client()
        self.enabled = os.getenv('REDIS_ENABLED', 'true').lower() == 'true'
        self.default_ttl = int(os.getenv('REDIS_CACHE_TTL', '300'))  # 5 minutes
        self.lock_timeout = 10  # Lock auto-expire after 10 seconds

    def is_available(self) -> bool:
        """Check if cache is available"""
        return self.enabled and self.redis_client is not None

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or cache unavailable
        """
        if not self.is_available():
            return None

        try:
            value = self.redis_client.get(key)
            if value is None:
                return None

            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Return raw value if not JSON
                return value

        except Exception as e:
            logger.error(f"Cache GET error for key '{key}': {e}")
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in cache

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized if possible)
            ttl: Time to live in seconds (default: REDIS_CACHE_TTL)

        Returns:
            True if successful, False otherwise
        """
        if not self.is_available():
            return False

        try:
            # Serialize to JSON if not a string
            if not isinstance(value, str):
                value = json.dumps(value)

            ttl = ttl or self.default_ttl
            self.redis_client.setex(key, ttl, value)
            return True

        except Exception as e:
            logger.error(f"Cache SET error for key '{key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete key from cache

        Args:
            key: Cache key to delete

        Returns:
            True if deleted, False otherwise
        """
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache DELETE error for key '{key}': {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern

        Args:
            pattern: Redis pattern (e.g., "products:*")

        Returns:
            Number of keys deleted
        """
        if not self.is_available():
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Cache DELETE PATTERN error for '{pattern}': {e}")
            return 0

    def clear_all(self) -> bool:
        """
        Clear all cache (use with caution!)

        Returns:
            True if successful
        """
        if not self.is_available():
            return False

        try:
            self.redis_client.flushdb()
            logger.warning("⚠️  All cache cleared!")
            return True
        except Exception as e:
            logger.error(f"Cache CLEAR ALL error: {e}")
            return False

    def get_or_set(
        self,
        key: str,
        callback: Callable[[], Any],
        ttl: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 0.1
    ) -> Any:
        """
        Get value from cache or compute and cache it with distributed stampede protection

        This method uses DISTRIBUTED Redis locks to ensure that when cache expires,
        only ONE worker/process/thread recomputes the value while others wait.
        This is safe for multi-worker deployments (unlike threading.Lock).

        Args:
            key: Cache key
            callback: Function to call if cache miss
            ttl: Time to live in seconds
            max_retries: Maximum retries to acquire lock
            retry_delay: Delay between retries in seconds

        Returns:
            Cached or computed value

        Example:
            # Without stampede protection (BAD):
            # Cache expires at 12:00:00
            # 100 requests across 8 workers at 12:00:01 -> 100 DB queries!

            # With distributed lock stampede protection (GOOD):
            # Cache expires at 12:00:00
            # Worker 1 / Request 1 acquires Redis lock, calls callback()
            # Workers 2-8 / Requests 2-100 wait and retry
            # All requests get the cached result
        """
        if not self.is_available():
            # Redis not available - compute directly without caching
            logger.warning(f"Redis unavailable, computing without cache: {key}")
            return callback()

        # Try to get from cache (fast path)
        cached_value = self.get(key)
        if cached_value is not None:
            logger.debug(f"Cache HIT: {key}")
            return cached_value

        # Cache miss - need to recompute with distributed stampede protection
        logger.debug(f"Cache MISS: {key}")

        # Build lock key
        lock_key = f"lock:{key}"

        # Try to acquire distributed lock
        for attempt in range(max_retries):
            # Try to set lock with NX (only if not exists) and EX (expiration)
            lock_acquired = self.redis_client.set(
                lock_key,
                "1",
                nx=True,  # Only set if key doesn't exist
                ex=self.lock_timeout  # Auto-expire after timeout
            )

            if lock_acquired:
                # We got the lock! Compute and cache the value
                logger.debug(f"Lock acquired for: {key}")
                try:
                    # Double-check cache (another process may have filled it)
                    cached_value = self.get(key)
                    if cached_value is not None:
                        logger.debug(f"Cache HIT after lock: {key}")
                        return cached_value

                    # Compute value
                    logger.info(f"Computing value for cache key: {key}")
                    value = callback()

                    # Store in cache
                    self.set(key, value, ttl)

                    return value

                except Exception as e:
                    logger.error(f"Error computing value for cache key '{key}': {e}")
                    raise

                finally:
                    # Always release the lock
                    try:
                        self.redis_client.delete(lock_key)
                        logger.debug(f"Lock released for: {key}")
                    except Exception as e:
                        logger.error(f"Error releasing lock for '{key}': {e}")

            else:
                # Lock already held by another process/worker
                # Wait a bit and retry to get cached result
                logger.debug(
                    f"Lock held by another process for '{key}', "
                    f"retry {attempt + 1}/{max_retries}"
                )
                time.sleep(retry_delay)

                # Check if cache was filled while waiting
                cached_value = self.get(key)
                if cached_value is not None:
                    logger.debug(f"Cache HIT after waiting: {key}")
                    return cached_value

        # Failed to acquire lock after all retries
        # Fallback: compute without lock (better than failing)
        logger.warning(
            f"Failed to acquire lock for '{key}' after {max_retries} retries, "
            f"computing without lock"
        )
        try:
            value = callback()
            # Try to cache anyway (best effort)
            self.set(key, value, ttl)
            return value
        except Exception as e:
            logger.error(f"Error in fallback computation for '{key}': {e}")
            raise

    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment counter (useful for rate limiting)

        Args:
            key: Counter key
            amount: Amount to increment

        Returns:
            New value or None if cache unavailable
        """
        if not self.is_available():
            return None

        try:
            return self.redis_client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Cache INCREMENT error for key '{key}': {e}")
            return None

    def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration on existing key

        Args:
            key: Cache key
            ttl: Time to live in seconds

        Returns:
            True if successful
        """
        if not self.is_available():
            return False

        try:
            return self.redis_client.expire(key, ttl)
        except Exception as e:
            logger.error(f"Cache EXPIRE error for key '{key}': {e}")
            return False

    def get_ttl(self, key: str) -> Optional[int]:
        """
        Get remaining TTL for key

        Args:
            key: Cache key

        Returns:
            Remaining seconds or None
        """
        if not self.is_available():
            return None

        try:
            ttl = self.redis_client.ttl(key)
            return ttl if ttl > 0 else None
        except Exception as e:
            logger.error(f"Cache GET TTL error for key '{key}': {e}")
            return None

    def build_key(self, prefix: str, *args, **kwargs) -> str:
        """
        Build cache key from components

        Args:
            prefix: Key prefix (e.g., "products")
            *args: Positional components
            **kwargs: Named components

        Returns:
            Formatted cache key

        Example:
            build_key("products", "list", skip=0, limit=10)
            => "products:list:skip:0:limit:10"
        """
        parts = [prefix]

        # Add positional args
        parts.extend(str(arg) for arg in args)

        # Add keyword args in sorted order for consistency
        for k, v in sorted(kwargs.items()):
            parts.extend([k, str(v)])

        return ":".join(parts)


# Global cache service instance
cache_service = CacheService()