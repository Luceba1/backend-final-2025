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
    Servicio de caché usando Redis (Upstash/Render compatible).
    Si no hay conexión, funciona sin explotar.
    """

    def __init__(self):
        # Intentar cliente Redis
        client = None
        try:
            client = get_redis_client()
            if client:
                client.ping()
        except Exception:
            client = None

        # Si Redis funciona → habilitado
        if client:
            self.enabled = True
            self.redis_client = client
            logger.info("✅ Redis enabled (Upstash/Render)")
        else:
            self.enabled = False
            self.redis_client = None
            logger.warning("⚠️ Redis disabled: no connection")

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
                decoded = json.loads(value)
                return decoded
            except:
                return None

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
    # DELETE PATTERN
    # -----------------------------
    def delete_pattern(self, pattern: str) -> int:
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
    # CLEAR
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
    # GET/SET with LOCK
    # -----------------------------
    def get_or_set(self, key: str, callback: Callable[[], Any], ttl: Optional[int] = None):
        if not self.is_available():
            return callback()
        cached = self.get(key)
        if cached is not None:
            return cached

        value = callback()
        self.set(key, value, ttl)
        return value

    # -----------------------------
    # BUILD KEY
    # -----------------------------
    def build_key(self, *parts, **kwargs) -> str:
        key_parts = list(map(str, parts))
        for k, v in kwargs.items():
            key_parts.append(str(k))
            key_parts.append(str(v))
        return ":".join(key_parts)


cache_service = CacheService()

