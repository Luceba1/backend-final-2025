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
    Cache totalmente compatible con Upstash Redis.
    No usa ninguna función que Upstash NO soporte.
    """

    def __init__(self):
        self.redis_client = get_redis_client()
        self.enabled = os.getenv("REDIS_ENABLED", "true").lower() == "true"
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))  # 5 min
        self.lock_timeout = 10  # segundos

    # -----------------------------
    # Estado
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
            raw = self.redis_client.get(key)
            if raw is None:
                return None

            # Upstash devuelve string literal
            try:
                data = json.loads(raw)
            except:
                return raw

            # TTL manual
            expires_at = data.get("expires_at")
            value = data.get("value")

            if expires_at and time.time() > expires_at:
                self.delete(key)
                return None

            return value

        except Exception as e:
            logger.error(f"Cache GET error for '{key}': {e}")
            return None

    # -----------------------------
    # SET (con TTL manual)
    # -----------------------------
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False

        try:
            expires_at = time.time() + (ttl or self.default_ttl)

            payload = json.dumps({
                "value": value,
                "expires_at": expires_at
            })

            self.redis_client.set(key, payload)
            return True

        except Exception as e:
            logger.error(f"Cache SET error for '{key}': {e}")
            return False

    # -----------------------------
    # DELETE
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
    # DELETE PATTERN (emulado sin KEYS)
    # -----------------------------
    def delete_pattern(self, pattern: str) -> int:
        """
        Emula un 'keys' usando SCAN (que Upstash sí soporta).
        """
        if not self.is_available():
            return 0

        try:
            deleted = 0
            cursor = "0"

            while True:
                cursor, keys = self.redis_client.scan(cursor=cursor, match=pattern)

                for k in keys:
                    self.redis_client.delete(k)
                    deleted += 1

                if cursor == "0":
                    break

            return deleted

        except Exception as e:
            logger.error(f"Cache DELETE PATTERN error for '{pattern}': {e}")
            return 0

    # -----------------------------
    # GET OR SET (lock distribuido)
    # -----------------------------
    def get_or_set(self, key: str, callback: Callable[[], Any], ttl: Optional[int] = None):
        if not self.is_available():
            return callback()

        cached = self.get(key)
        if cached is not None:
            return cached

        lock_key = f"lock:{key}"

        try:
            lock = self.redis_client.set(lock_key, "1", nx=True, ex=self.lock_timeout)
        except:
            return callback()

        if lock:
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
                except:
                    pass

        # si no tomó lock, esperar y reintentar acceso al cache
        time.sleep(0.1)
        cached = self.get(key)
        if cached is not None:
            return cached

        value = callback()
        self.set(key, value, ttl)
        return value

    # -----------------------------
    # BUILD KEY (compatible total)
    # -----------------------------
    def build_key(self, *parts, **kwargs) -> str:
        key_parts = [str(p) for p in parts]

        for k, v in kwargs.items():
            key_parts.append(str(k))
            key_parts.append(str(v))

        return ":".join(key_parts)


cache_service = CacheService()

