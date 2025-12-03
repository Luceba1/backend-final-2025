import json
import time
import logging
from typing import Any, Optional
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
                # test
                self.redis.ping()
                self.available = True
                logger.info("Upstash Redis conectado correctamente.")
            else:
                logger.warning("Redis no configurado, cache deshabilitado.")
        except Exception as e:
            logger.error(f"Redis no disponible: {e}")
            self.available = False

    def is_available(self) -> bool:
        return self.available

    # --------------------------------------------------------------------------------
    #   IMPLEMENTACIÓN PROPIA DE TTL (porque Upstash no usa expire())
    # --------------------------------------------------------------------------------

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        if not self.is_available():
            return

        try:
            expires_at = time.time() + ttl_seconds

            # Guardamos un bloque JSON con el valor + timestamp de expiración
            payload = json.dumps({
                "value": value,
                "expires_at": expires_at
            })

            self.redis.set(key, payload)

        except Exception as e:
            logger.error(f"Redis SET error: {e}")

    def _safe_get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None

        try:
            raw = self.redis.get(key)

            if raw is None:
                return None

            # Upstash devuelve strings → los convertimos
            try:
                data = json.loads(raw)
            except:
                logger.error("Error JSON decode interno en cache.")
                return None

            expires_at = data.get("expires_at")
            value = data.get("value")

            # Expirado → lo eliminamos
            if expires_at and time.time() > expires_at:
                self.redis.delete(key)
                return None

            # El valor puede venir como string JSON → convertimos
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except:
                    # si no es JSON, retornamos el string intacto
                    return value

            return value

        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def get(self, key: str) -> Optional[Any]:
        return self._safe_get(key)

    def delete(self, key: str) -> None:
        if not self.is_available():
            return
        try:
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis DEL error: {e}")

    def clear_prefix(self, prefix: str) -> None:
        """
        Borra todas las claves que empiecen con cierto prefijo.
        Útil para cuando creas/actualizas/eliminás productos o categorías.
        """
        if not self.is_available():
            return

        try:
            # Upstash no soporta KEYS, así que usamos SCAN
            cursor = "0"
            while cursor != 0:
                cursor, keys = self.redis.scan(cursor=cursor, match=f"{prefix}*")
                for k in keys:
                    self.redis.delete(k)

        except Exception as e:
            logger.error(f"Redis CLEAR PREFIX error: {e}")


# Instancia global (lo que importan los servicios)
cache_service = CacheService()



