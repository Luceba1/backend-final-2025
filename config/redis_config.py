"""
Upstash Redis configuration — sync-safe version for Render.
Uses REST API instead of redis.asyncio.
Fully compatible with CacheService sync wrapper.
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)


class UpstashRedisSync:
    """Synchronous wrapper for Upstash Redis (REST API)."""

    def __init__(self):
        raw_url = os.getenv("UPSTASH_REDIS_REST_URL")
        token = os.getenv("UPSTASH_REDIS_REST_TOKEN")

        if not raw_url or not token:
            logger.warning("⚠️ Upstash Redis NOT configured")
            self.enabled = False
            return

        # URL correcta (añadir /redis siempre)
        self.url = raw_url.rstrip("/") + "/redis"
        self.token = token

        logger.info("✅ Upstash Redis REST client configured")
        self.enabled = True

    # ---- PRIVATE HEADER ----
    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    # ---- GET ----
    def get(self, key: str):
        if not self.enabled:
            return None

        try:
            r = requests.get(f"{self.url}/get/{key}", headers=self._headers())
            data = r.json()
            return data.get("result")
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    # ---- SET ----
    def set(self, key: str, value: str, ttl=None):
        if not self.enabled:
            return False

        try:
            payload = {"value": value}
            if ttl:
                payload["ex"] = ttl

            r = requests.post(
                f"{self.url}/set/{key}",
                headers=self._headers(),
                json=payload
            )
            return r.status_code == 200
        except Exception as e:
            logger.error(f"Redis SET error: {e}")
            return False

    # ---- DELETE ----
    def delete(self, key: str):
        if not self.enabled:
            return False

        try:
            r = requests.post(
                f"{self.url}/del/{key}",
                headers=self._headers()
            )
            return r.status_code == 200
        except Exception as e:
            logger.error(f"Redis DEL error: {e}")
            return False

    # ---- KEYS ----
    def keys(self, pattern: str):
        return []

    # ---- CLEAR DB ----
    def flushdb(self):
        return False

    # ---- HEALTH CHECK ----
    def is_available(self) -> bool:
        if not self.enabled:
            return False

        try:
            test_key = "__healthcheck__"
            self.set(test_key, "ok", ttl=5)
            return self.get(test_key) == "ok"
        except Exception:
            return False


redis_client = UpstashRedisSync()


def get_redis_client():
    return redis_client


def check_redis_connection() -> bool:
    return redis_client.is_available()


