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
    # INTERNAL HELPERS (Upstash-safe wrappers)
    # -------------------------------------------------------

    def _safe_get(self, key: str):
        try:
            raw = self.redis.get(key)
            if raw is None:
                return None
            # Upstash devuelve {"value":"..."} → devolvemos .get("value")
            if isinstance(raw, dict) and "value" in raw:
                raw = raw["value"]
            try:
                return json.loads(raw)
            except:
                return raw
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def _safe_set(self, key: str, value: Any, ttl: int):
        try:
            if not isinstance(value, str):
                value = json.dumps(value)

            # Upstash usa set(key, value) y no soporta setex()
            self.redis.set(key, value)

            # TTL manual
            self.redis.expire(key, ttl)
        except Exception as e:
            logger.error(f"Redis SET error: {e}")

    def _safe_incr(self, key: str):
        try:
            return self.redis.set(key, "1")  # fallback simple
        except:
            return None

    def _safe_expire(self, key: str, ttl: int):
        try:
            self.redis.expire(key, ttl)
        except:
            pass

    # -----------------------------------------------
    # PUBLIC API (no se toca nada afuera)
    # -----------------------------------------------

    def is_available(self) -> bool:
        return self.enabled and self.redis is not None

    def get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None
        return self._safe_get(key)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False
        ttl = ttl or self.default_ttl
        self._safe_set(key, value, ttl)
        return True

    def delete(self, key: str) -> bool:
        try:
            self.redis.delete(key)
            return True
        except:
            return False

    def delete_pattern(self, pattern: str) -> int:
        # Upstash no soporta KEYS → ignoramos la operación
        return 0

    def clear_all(self) -> bool:
        # Upstash no soporta flushDB
        return False

    def get_or_set(
        self, key: str, callback: Callable[[], Any],
        ttl: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 0.1
    ) -> Any:

        ttl = ttl or self.default_ttl

        if not self.is_available():
            return callback()

        cached = self.get(key)
        if cached is not None:
            return cached

        lock_key = f"lock:{key}"

        for attempt in range(max_retries):
            got_lock = self.redis.set(lock_key, "1", nx=True)
            if got_lock:
                try:
                    cached_late = self.get(key)
                    if cached_late is not None:
                        return cached_late
                    value = callback()
                    self.set(key, value, ttl)
                    return value
                finally:
                    try:
                        self.redis.delete(lock_key)
                    except:
                        pass
            else:
                time.sleep(retry_delay)
                cached_retry = self.get(key)
                if cached_retry is not None:
                    return cached_retry

        # fallback final
        value = callback()
        self.set(key, value, ttl)
        return value

    def increment(self, key: str, amount: int = 1):
        return self._safe_incr(key)

    def expire(self, key: str, ttl: int) -> bool:
        self._safe_expire(key, ttl)
        return True

    def get_ttl(self, key: str):
        # Upstash no soporta TTL → devolvemos None
        return None

    def build_key(self, prefix: str, *args, **kwargs) -> str:
        parts = [prefix]
        parts.extend(str(a) for a in args)
        for k, v in sorted(kwargs.items()):
            parts.extend([k, str(v)])
        return ":".join(parts)


cache_service = CacheService()


