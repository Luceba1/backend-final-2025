import os
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)


redis_client = None


def get_redis_client():
    """
    Returns a native Redis client (Redis over TLS)
    compatible with Upstash + Render.
    """
    global redis_client

    if redis_client:
        return redis_client

    redis_url = os.getenv("REDIS_URL")

    if not redis_url:
        logger.warning("⚠️ No REDIS_URL set. Redis disabled.")
        return None

    try:
        client = redis.from_url(
            redis_url,
            decode_responses=True,
            encoding="utf-8",
        )

        # Probar conexión inicial
        # Si falla, no rompe el backend
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            loop.run_until_complete(client.ping())
            logger.info("✅ Connected to Upstash Redis (TLS)")
        except Exception as e:
            logger.error(f"❌ Redis ping failed: {e}")
            return None

        redis_client = client
        return redis_client

    except Exception as e:
        logger.error(f"❌ Redis init error: {e}")
        return None


def check_redis_connection() -> bool:
    """Used by /health endpoint for Redis check."""
    try:
        client = get_redis_client()
        if not client:
            return False

        import asyncio
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(client.ping()) is True

    except Exception:
        return False
