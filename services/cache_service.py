import json
import time
import logging
from typing import Any, Optional
from upstash_redis import Redis

logger = logging.getLogger("services.cache_service")


class CacheService:
    def __init__(self):
        # CONFIGURACIÓN ORIGINAL QUE FUNCIONABA
        try:
            self.redis = Redis(
                url="YOUR_UPSTASH_URL",
                token="YOUR_UPSTASH_TOKEN"
            )
            self.redis.ping()
            self.available = True
            logger.info("Redis conectado correctamente.")
        except Exception as e:
            logger.error(f"Redis no disponible: {e}")
            self.redis = None
            self.available = False

    def is_available(self) -> bool:
        return self.available

    def build_key(self, *parts) -> str:
        return ":".join(str(p) for p in parts)

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        if not self.is_available():
            return

        try:
            expires_at = time.time() + ttl_seconds
            payload = json.dumps({
                "value": value,
                "expires_at": expires_at
            })
            self.redis.set(key, payload)
        except Exception as e:
            logger.error(f"Redis SET error: {e}")

    def get(self, key: str) -> Optional[Any]:
        if not self.is_available():
            return None

        try:
            raw = self.redis.get(key)
            if raw is None:
                return None

            data = json.loads(raw)
            expires_at = data.get("expires_at")
            value = data.get("value")

            if expires_at and time.time() > expires_at:
                self.redis.delete(key)
                return None

            return value

        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def delete(self, key: str) -> None:
        if not self.is_available():
            return
        try:
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis DEL error: {e}")

    def clear_prefix(self, prefix: str) -> None:
        if not self.is_available():
            return
        try:
            cursor = "0"
            while cursor != 0:
                cursor, keys = self.redis.scan(cursor=cursor, match=f"{prefix}*")
                for k in keys:
                    self.redis.delete(k)
        except Exception as e:
            logger.error(f"Redis CLEAR PREFIX error: {e}")


cache_service = CacheService()

