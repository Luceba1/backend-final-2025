import json
import logging
import os
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Any, Callable

from config.redis_config import get_redis_client
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


# ----------------------------------------
# JSON ENCODER QUE SOPORTA datetime y Decimal
# ----------------------------------------
def safe_json_encode(obj):
    """
    Convierte automáticamente:
    - datetime → ISO string
    - date → ISO string
    - Decimal → float
    - Objetos → dict
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if hasattr(obj, "model_dump"):
        return obj.model_dump()

    if hasattr(obj, "__dict__"):
        return obj.__dict__

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ----------------------------------------
# CACHE SERVICE (para Upstash REST)
# ----------------------------------------
class CacheService:
    """
    Cache sincronizado usando Upstash REST.
    Totalmente compatible con servicios sync de FastAPI.
    """

    def __init__(self):
        self.redis_client = None
        self.enabled = False
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "300"))

    async def init(self):
        """
        Inicialización simple. No realiza ping.
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
            if not value:
                return None

            return json.loads(value)

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
            # Convertimos el valor a JSON seguro
            encoded = json.dumps(value, default=safe_json_encode)

            ttl = ttl or self.default_ttl
            return self.redis_client.set(key, encoded, ttl)

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
    # DELETE PATTERN (no soportado)
    # -----------------------------
    def delete_pattern(self, pattern: str) -> int:
        logger.info("⚠️ delete_pattern ignorado (Upstash REST no soporta KEYS)")
        return 0

    # -----------------------------
    # CLEAR ALL (no soportado)
    # -----------------------------
    def clear_all(self) -> bool:
        logger.info("⚠️ clear_all no disponible en Upstash REST")
        return False

    # -----------------------------
    # GET OR SET
    # -----------------------------
    def get_or_set(self, key: str, callback: Callable[[], Any], ttl: Optional[int] = None):
        if not self.is_available():
            return callback()

        cached = self.get(key)
        if cached is not None:
            return cached

        # Obtener valor desde DB
        value = callback()

        # Guardar en Redis
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


