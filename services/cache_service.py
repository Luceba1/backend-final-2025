import json
import logging
import os
import time
from typing import Optional, Any, Callable

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CacheService:
    """
    Cache compatible con Upstash y con TODO tu backend actual.
    """

    def __init__(self):
        self.redis = get_redis_client()
        self.enabled = os.getenv("REDIS_ENABLED", "true").lower() == "true"
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))  # 5 min
        self.lock_timeout = 10

    def is_available(self) -> bool:
        return self.enabled and self.redis is not None

    # -----------------------------------
    # GET
    # -----------------------------------
    def get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None

        try:
            value = self.redis.get(key)

            if value is None:
                return None

            try:
                return json.loads(value)
            except:
                return None  # Nunca devolvemos strings sueltas

        except Exception as e:
            logger.error(f"Cache GET error for '{key}': {e}")
            return None

    # -----------------------------------
    # SET
    # -----------------------------------
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False

        try:
            serialized = json.dumps(value)
            ttl = ttl or self.default_ttl

            # Upstash
            self.redis.set(key, serialized)
            self.redis.expire(key, ttl)

            return True

        except Exception as e:
            logger.error(f"Cache SET error for '{key}': {e}")
            return False

    # -----------------------------------
    # DELETE PATTERN (SCAN, compatible Upstash)
    # -----------------------------------
    def delete_pattern(self, pattern: str) -> int:
        if not self.is_available():
            return 0

        try:
            cursor = 0
            total_deleted = 0

            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)

                if keys:
                    self.redis.delete(*keys)
                    total_deleted += len(keys)

                if cursor == 0:
                    break

            return total_deleted

        except Exception as e:
            logger.error(f"Cache DELETE PATTERN error for '{pattern}': {e}")
            return 0

    # -----------------------------------
    # BUILD KEY
    # -----------------------------------
    def build_key(self, *parts, **kwargs) -> str:
        segments = [str(p) for p in parts]

        for k, v in kwargs.items():
            segments.append(str(k))
            segments.append(str(v))

        return ":".join(segments)


cache_service = CacheService()

