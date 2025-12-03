"""
Cache Service Module

Provides high-level caching operations using Redis with automatic
serialization, TTL management, error handling, and distributed cache stampede protection.
"""
import json
import logging
import time
from typing import Optional, Any, Callable
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
        """Get value from cache"""
        if not self.is_available():
            return None

        try:
            value = self.redis_client.get(key)
            if value is None:
                return None

            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value

        except Exception as e:
            logger.error(f"Cache GET error for key '{key}': {e}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with TTL"""
        if not self.is_available():
            return False

        try:
            if not isinstance(value, str):
                value = json.dumps(value)

            ttl = ttl or self.default_ttl
            self.redis_client.setex(key, ttl, value)
            return True

        except Exception as e:
            logger.error(f"Cache SET error for key '{key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """Delete a key"""
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache DELETE error for key '{key}': {e}")
            return False

    # ----------------------------------------------------------------------
    # ⭐ NUEVO — COMPATIBILIDAD TOTAL CON TU BACKEND
    # ----------------------------------------------------------------------
    def delete_pattern(self, pattern: str) -> int:
        """
        Compatibilidad con servicios que esperan delete_pattern().
        Borra todas las keys que coincidan con el patrón.
        """
        if not self.is_available():
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                return deleted
            return 0
        except Exception as e:
            logger.error(f"Cache DELETE PATTERN error for '{pattern}': {e}")
            return 0
    # ----------------------------------------------------------------------

    def clear_all(self) -> bool:
        """Clear all cache"""
        if not self.is_available():
            return False

        try:
            self.redis_client.flushdb()
            logger.warning("⚠️ All cache cleared!")
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
        Get or compute value with stampede protection using distributed Redis locks.
        """
        if not self.is_available():
            return callback()

        cached = self.get(key)
        if cached is not None:
            return cached

        lock_key = f"lock:{key}"

        for attempt in range(max_retries):
            lock_acquired = self.redis_client.set(
                lock_key, "1", nx=True, ex=self.lock_timeout
            )

            if lock_acquired:
                try:
                    cached = self.get(key)
                    if cached is not None:
                        return cached

                    value = callback()

                    self.set(key, value, ttl)

                    return value

                finally:
                    try:
                        self.redis_client.delete(lock_key)
                    except Exception as e:
                        logger.error(f"Error releasing lock for '{key}': {e}")

            else:
                time.sleep(retry_delay)
                cached = self.get(key)
                if cached is not None:
                    return cached

        value = callback()
        self.set(key, value, ttl)
        return value

    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        if not self.is_available():
            return None

        try:
            return self.redis_client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Cache INCREMENT error for key '{key}': {e}")
            return None

    def expire(self, key: str, ttl: int) -> bool:
        if not self.is_available():
            return False

        try:
            return self.redis_client.expire(key, ttl)
        except Exception as e:
            logger.error(f"Cache EXPIRE error for key '{key}': {e}")
            return False

    def get_ttl(self, key: str) -> Optional[int]:
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
        Build consistent cache keys
        """
        parts = [prefix]

        parts.extend(str(arg) for arg in args)

        for k, v in sorted(kwargs.items()):
            parts.extend([k, str(v)])

        return ":".join(parts)


# Global instance
cache_service = CacheService()
