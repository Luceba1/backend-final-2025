import json
import logging
import os
import asyncio
from typing import Optional, Any, Callable

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CacheService:
    """
    Cache síncrono usando Redis asyncio por debajo.
    Compatible con todos los servicios existentes (sync).
    """

    def __init__(self):
        self.redis_client = None
        self.enabled = False
        self.loop = None  # event loop interno

    async def init(self):
        """
        Inicialización asíncrona (se llama en startup).
        Prepara el cliente Redis y el event loop.
        """
        try:
            client = get_redis_client()
            if client:
                await client.ping()
                self.redis_client = client
                self.enabled = True
                logger.info("✅ Redis enabled (Upstash/Render)")
            else:
                logger.warning("⚠️ Redis disabled: no client available")

            # Creamos un event loop propio
            try:
                self.loop = asyncio.get_event_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

        except Exception as e:
            logger.error(f"❌ Redis init failed: {e}")
            self.enabled = False

        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))

    # -----------------------------
    # ESTADO
    # -----------------------------
    def is_available(self) -> bool:
        return self.enabled and self.redis_client is not None

    # -----------------------------
    # HELPER SYNC-WRAPPER
    # -----------------------------
    def _run(self, coro):
        """
        Ejecuta coroutines ASYNC como SYNC.
        """
        try:
            return self.loop.run_until_complete(coro)
        except RuntimeError:
            # Si no hay loop activo, creamos uno
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)

    # -----------------------------
    # GET
    # -----------------------------
    def get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None
        try:
            value = self._run(self.redis_client.get(key))
            if value is None:
                return None

            try:
                return json.loads(value)
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
            self._run(self.redis_client.setex(key, ttl, value))
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
            self._run(self.redis_client.delete(key))
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
            keys = self._run(self.redis_client.keys(pattern))
            if keys:
                return self._run(self.redis_client.delete(*keys))
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
            self._run(self.redis_client.flushdb())
            return True
        except Exception as e:
            logger.error(f"Cache CLEAR ALL error: {e}")
            return False

    # -----------------------------
    # GET/SET con callback (SYNC)
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
