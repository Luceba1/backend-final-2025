"""
Unified Cache Service for Upstash Redis
Fully compatible with your backend (categories, products, orders, clients, etc.)
Prevents double JSON encoding issues and supports delete_pattern().
"""

import json
import logging
import time
from typing import Optional, Any
from upstash_redis import Redis
from config.settings import Settings

logger = logging.getLogger("services.cache_service")


class CacheService:
    def __init__(self):
        settings = Settings()

        self.redis = None
        self.available = False

        try:
            if settings.redis_url and settings.redis_token:
                self.redis = Redis(
                    url=settings.redis_url,
                    token=settings.redis_token
                )
                self.redis.ping()
                self.available = True
                logger.info("Upstash Redis conectado correctamente.")
            else:
                logger.warning("Redis no configurado → cache deshabilitado.")
        except Exception as e:
            logger.error(f"Redis no disponible: {e}")
            self.available = False

        self.default_ttl = 300  # 5 minutos

    # -------------------------------------------------------------------------
    # ✔️ Generar claves uniforme
    # -------------------------------------------------------------------------
    def build_key(self, prefix: str, *args, **kwargs) -> str:
        parts = [prefix]
        parts.extend(str(arg) for arg in args)

        for k, v in sorted(kwargs.items()):
            parts.extend([k, str(v)])

        return ":".join(parts)

    # -------------------------------------------------------------------------
    # ✔️ SET con TTL (formato propio porque Upstash no tiene expire tradicional)
    # -------------------------------------------------------------------------
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        if not self.available:
            return

        try:
            ttl = ttl or self.default_ttl
            expires_at = time.time() + ttl

            # Se garantiza que NO quede doble JSON
            payload = {
                "value": value,
                "expires_at": expires_at
            }

            self.redis.set(key, json.dumps(payload))

        except Exception as e:
            logger.error(f"Redis SET error '{key}': {e}")

    # -------------------------------------------------------------------------
    # ✔️ GET seguro (evita doble JSON)
    # -------------------------------------------------------------------------
    def _safe_get(self, key: str) -> Optional[Any]:
        if not self.available:
            return None

        try:
            raw = self.redis.get(key)
            if raw is None:
                return None

            # Intentamos decodificar el JSON completo
            try:
                data = json.loads(raw)
            except:
                logger.error(f"Error decodificando JSON desde cache: {key}")
                return None

            # Revisar expiración manual
            expires_at = data.get("expires_at")
            if expires_at and time.time() > expires_at:
                self.redis.delete(key)
                return None

            value = data.get("value")

            # ⚠️ FIX: si el value es un JSON string → lo decodificamos de nuevo
            if isinstance(value, str):
                try:
                    decoded = json.loads(value)
                    return decoded
                except:
                    return value

            return value

        except Exception as e:
            logger.error(f"Redis GET error '{key}': {e}")
            return None

    def get(self, key: str) -> Optional[Any]:
        return self._safe_get(key)

    # -------------------------------------------------------------------------
    # ✔️ Delete simple
    # -------------------------------------------------------------------------
    def delete(self, key: str) -> None:
        if not self.available:
            return
        try:
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis DEL error '{key}': {e}")

    # -------------------------------------------------------------------------
    # ✔️ Delete por patrón (match)
    # -------------------------------------------------------------------------
    def delete_pattern(self, pattern: str) -> int:
        """
        Elimina TODAS las claves que coincidan con pattern.
        Ejemplo: delete_pattern("products:*")
        """
        if not self.available:
            return 0

        try:
            cursor = "0"
            deleted = 0

            while cursor != 0:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern)
                if keys:
                    for k in keys:
                        self.redis.delete(k)
                        deleted += 1

            return deleted

        except Exception as e:
            logger.error(f"Redis DELETE_PATTERN error '{pattern}': {e}")
            return 0


# -------------------------------------------------------------------------
# Instancia global para usar en ProductService, CategoryService, etc.
# -------------------------------------------------------------------------
cache_service = CacheService()

