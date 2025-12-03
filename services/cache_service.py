import json
import logging
import time
from typing import Optional, Any, Callable
import os

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CacheService:

    def __init__(self):
        self.redis = get_redis_client()
        self.enabled = os.getenv("REDIS_ENABLED", "true").lower() == "true"
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))
        self.lock_timeout = 10

    # -------------------------------------------------------
    # INTERNAL HELPERS — Upstash-safe (NO expire, NO setex)
    # -------------------------------------------------------

    def _safe_get(self, key: str):
        """
        Usa TTL manual: guardamos {value, expires_at} en JSON.
        """
        try:
            raw = self.redis.get(key)
            if raw is None:
                return None

            if isinstance(raw, dict) and "value" in raw:
                raw = raw["value"]

            data = json.loads(raw)

            # TTL manual
            expires_at = data.get("expires_at")
            if expires_at and time.time() > expires_at:
                self.redis.delete(key)
                return None

            return data.get("value")

        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def _safe_set(self, key: str, value: Any, ttl: int):
        """
        Guardamos nuestro propio TTL manual dentro del JSON.
        """
        try:
            expires_at = time.time() + ttl
            payload = {
                "value": value,
                "expires_at": expires_at
            }
            self.redis.set(key, json.dumps(payload))
        except Exception as e:
            logger.error(f"Redis SET error: {e}")

    # -----------------------------------------------
    # PUBLIC API (sin romper otras clases)
    # -----------------------------------------------

    def is_available(self) -> bool:
        return self.enabled and self.redis is not None

    def get(self, key: str) -> Optional[Any]:
        return self._safe_get(key) if self.is_available() else None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False
        self._safe_set(key, value, ttl or self.default_ttl)
        return True

    def delete(self, key: str) -> bool:
        try:
            self.redis.delete(key)
            return True
        except:
            return False

    def delete_pattern(self, pattern: str) -> int:
        # Upstash no soporta KEYS
        return 0

    def clear_all(self) -> bool:
        # Upstash no soporta FLUSHDB
        return False

    def get_or_set(
        self, key: str, callback: Callable[[], Any],
        ttl: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 0.1
    ) -> Any:

        ttl = ttl or self.default_ttl

        cached = self.get(key)
        if cached is not None:
            return cached

        value = callback()
        self.set(key, value, ttl)
        return value

    def increment(self, key: str, amount: int = 1):
        # Upstash no soporta INCRBY
        return None

    def expire(self, key: str, ttl: int) -> bool:
        # TTL manual, nada que hacer
        return True

    def get_ttl(self, key: str):
        # TTL manual, no se calcula exacto
        return None

    def build_key(self, prefix: str, *args, **kwargs) -> str:
        parts = [prefix]
        parts.extend(str(a) for a in args)
        for k, v in sorted(kwargs.items()):
            parts.extend([k, str(v)])
        return ":".join(parts)


cache_service = CacheService()


