"""
Redis Configuration Module (Upstash REST Version)

Compatible with Render + Upstash.
Uses HTTPS REST requests instead of TCP sockets.
"""

import os
import json
import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class UpstashRedis:
    """
    Upstash Redis client using REST API.
    Fully compatible with Render.
    """

    def __init__(self):
        self.url = os.getenv("UPSTASH_REDIS_REST_URL")
        self.token = os.getenv("UPSTASH_REDIS_REST_TOKEN")

        if not self.url or not self.token:
            logger.warning("⚠️ Redis (Upstash) not configured. Running without cache.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("✅ Upstash Redis configured successfully.")

    # ----------------------------- BASIC WRAPPERS -----------------------------

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get(self, key: str) -> Optional[str]:
        if not self.enabled:
            return None
        try:
            res = requests.get(f"{self.url}/get/{key}", headers=self._headers())
            data = res.json()
            return data.get("result")
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def set(self, key: str, value: str) -> bool:
        if not self.enabled:
            return False
        try:
            payload = {"value": value}
            res = requests.post(f"{self.url}/set/{key}",
                                headers=self._headers(),
                                data=json.dumps(payload))
            return res.status_code == 200
        except Exception as e:
            logger.error(f"Redis SET error: {e}")
            return False

    def delete(self, key: str) -> bool:
        if not self.enabled:
            return False
        try:
            res = requests.post(f"{self.url}/del/{key}",
                                headers=self._headers())
            return res.status_code == 200
        except Exception as e:
            logger.error(f"Redis DEL error: {e}")
            return False

    # ----------------------------- HEALTH CHECK -----------------------------

    def is_available(self) -> bool:
        if not self.enabled:
            return False
        try:
            test_key = "__healthcheck__"
            self.set(test_key, "ok")
            return self.get(test_key) == "ok"
        except Exception:
            return False


# Global instance
redis_client = UpstashRedis()


def get_redis_client() -> UpstashRedis:
    """Compatibility wrapper for dependency injection."""
    return redis_client


def check_redis_connection() -> bool:
    """Health check used by your FastAPI /health endpoint."""
    return redis_client.is_available()
