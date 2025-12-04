import json
import logging
import os
from typing import Optional, Any, Callable

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CacheService:
    """
    Cache síncrono usando Upstash Redis vía REST.
    Compatible con todos los servicios existentes.
    """

    def __init__(self):
        self.redis_client = None
        self.enabled = False
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))

    async def init(self):
        """
        Inicializa el cliente Redis (REST).
        No usa async real, solo prepara el cliente.
        """
        try:
            client = get_redis_client()

            if client and client.enabled:
                self.redis_client = client
                self.enabled = True
                logger.info("✅ Redis enabled (Upstash REST)")
            else:
                logger.warning("⚠️ Redis disabled: no client available")
                self.enabled = False

        except Exception as e:
            logger.error(f"❌ Redis init failed: {e}")
            self.enabled = False

    # -----------------------------
    # ESTADO
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
                return None

        except Exception as e:
            logger.error(f"Cache GET error for '{key}': {e}")
            return None

    # -----------------------------
    # SET (VERSIÓN FINAL)
    # -----------------------------
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.is_available():
            return False

        try:
            # Conversor universal JSON-safe
            def default(o):
                import datetime
                from decimal import Decimal
                from types import MappingProxyType

                # mappingproxy (SQLAlchemy metadata)
                if isinstance(o, MappingProxyType):
                    return dict(o)

                # datetime / date
                if isinstance(o, (datetime.datetime, datetime.date)):
                    return o.isoformat()

                # Decimal
                if isinstance(o, Decimal):
                    return float(o)

                # Pydantic v2
                if hasattr(o, "model_dump"):
                    return o.model_dump()

                # ORM object
                if hasattr(o, "__dict__"):
                    return {k: v for k, v in o.__dict__.items() if not k.startswith("_")}

                return str(o)

            # Convertir a JSON seguro
            if not isinstance(value, str):
                value = json.dumps(value, default=default)

            ttl = ttl or self.default_ttl

            return self.redis_client.set(key, value, ttl)

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
            return self.redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache DELETE error: {e}")
            return False

    # -----------------------------
    # DELETE PATTERN (NO DISPONIBLE)
    # -----------------------------
    def delete_pattern(self, pattern: str) -> int:
        logger.info("⚠️ delete_pattern ignorado (Upstash REST no soporta KEYS)")
        return 0

    # -----------------------------
    # CLEAR ALL (NO DISPONIBLE)
    # -----------------------------
    def clear_all(self) -> bool:
        logger.info("⚠️ clear_all no disponible en Upstash REST")
        return False

    # -----------------------------
    # GET/SET
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

