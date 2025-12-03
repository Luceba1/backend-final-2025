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
    Servicio de caché seguro y compatible con todo el backend original.
    """

    def __init__(self):
        self.redis_client = get_redis_client()
        self.enabled = os.getenv("REDIS_ENABLED", "true").lower() == "true"
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))  # 5 min
        self.lock_timeout = 10

    # -----------------------------
    # ESTADO BÁSICO
    # -----------------------------
    def is_available(self) -> bool:
        return self.enabled and self.redis_client is not None

    # -----------------------------
    # GET
    # -----------------------------
    def get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None

        try:
            value = self.redis_client.get(key)
            if value is None:
                return None

            try:
                return json.loads(value)
            except:
                return value

        except Exception as e:
            logger.error(f"Cache GET error for '{key}': {e}")
            return None

    # -----------------------------
    # SET
    # -----------------------------
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False

        try:
            if not isinstance(value, str):
                value = json.dumps(value)

            ttl = ttl or self.default_ttl
            self.redis_client.setex(key, ttl, value)
            return True

        except Exception as e:
            logger.error(f"Cache SET error for '{key}': {e}")
            return False

    # -----------------------------
    # DELETE KEY
    # -----------------------------
    def delete(self, key: str) -> bool:
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache DELETE error for '{key}': {e}")
            return False

    # -----------------------------
    # DELETE PATTERN (NECESARIO PARA SERVICES)
    # -----------------------------
    def delete_pattern(self, pattern: str) -> int:
        """
        El backend original usa esta función.
        Upstash soporta KEYS, así que es seguro.
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

    # -----------------------------
    # CLEAR ALL
    # -----------------------------
    def clear_all(self) -> bool:
        if not self.is_available():
            return False

        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Cache CLEAR ALL error: {e}")
            return False

    # -----------------------------
    # GET OR SET (con lock distribuido)
    # -----------------------------
    def get_or_set(
        self,
        key: str,
        callback: Callable[[], Any],
        ttl: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 0.1,
    ):
        if not self.is_available():
            return callback()

        cached_value = self.get(key)
        if cached_value is not None:
            return cached_value

        lock_key = f"lock:{key}"

        for attempt in range(max_retries):
            try:
                lock_acquired = self.redis_client.set(
                    lock_key,
                    "1",
                    nx=True,
                    ex=self.lock_timeout,
                )
            except Exception:
                return callback()

            if lock_acquired:
                try:
                    cached_value = self.get(key)
                    if cached_value is not None:
                        return cached_value

                    value = callback()
                    self.set(key, value, ttl)
                    return value

                finally:
                    try:
                        self.redis_client.delete(lock_key)
                    except:
                        pass

            time.sleep(retry_delay)

            cached_value = self.get(key)
            if cached_value is not None:
                return cached_value

        value = callback()
        self.set(key, value, ttl)
        return value

    # -----------------------------
    # INCREMENT
    # -----------------------------
    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        if not self.is_available():
            return None

        try:
            return self.redis_client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Cache INCREMENT error for '{key}': {e}")
            return None

    # -----------------------------
    # EXPIRE
    # -----------------------------
    def expire(self, key: str, ttl: int) -> bool:
        if not self.is_available():
            return False

        try:
            return self.redis_client.expire(key, ttl)
        except Exception as e:
            logger.error(f"Cache EXPIRE error for '{key}': {e}")
            return False

    # -----------------------------
    # TTL
    # -----------------------------
    def get_ttl(self, key: str) -> Optional[int]:
        if not self.is_available():
            return None

        try:
            ttl = self.redis_client.ttl(key)
            return ttl if ttl > 0 else None
        except Exception as e:
            logger.error(f"Cache TTL error: {e}")
            return None

    # -----------------------------
    # BUILD KEY (COMPATIBLE CON TODO TU BACKEND)
    # -----------------------------
    def build_key(self, *parts, **kwargs) -> str:
        """
        Compatible con:
            build_key("categories", "list", skip=0, limit=20)
        """
        key_parts = [str(p) for p in parts]

        for k, v in kwargs.items():
            key_parts.append(str(k))
            key_parts.append(str(v))

        return ":".join(key_parts)


cache_service = CacheService()


