import json
import logging
from typing import Any, Optional
from config.redis_config import redis_client

logger = logging.getLogger(__name__)


# ============================================================
# 🔥 Helpers internos para Upstash
# ============================================================

async def _send(cmd: list):
    """Upstash ejecuta los comandos como listas tipo ["SET", key, val]."""
    try:
        res = await redis_client.send(cmd)
        if isinstance(res, dict) and "result" in res:
            return res["result"]
        return res
    except Exception as e:
        logger.error(f"Redis command error {cmd}: {e}")
        return None


async def _get(key: str):
    try:
        res = await redis_client.get(key)
        if isinstance(res, dict) and "result" in res:
            return res["result"]
        return res
    except Exception as e:
        logger.error(f"Redis GET error for {key}: {e}")
        return None


# ============================================================
# 🔥 Simulaciones de métodos que tus clases ya usan
# ============================================================

async def setex(key: str, ttl: int, value: str):
    """
    Simula redis.setex(key, ttl, value)
    → SET key value EX ttl
    """
    return await _send(["SET", key, value, "EX", str(ttl)])


async def expire(key: str, ttl: int):
    """
    Simula redis.expire(key, ttl)
    → EXPIRE key ttl
    """
    return await _send(["EXPIRE", key, str(ttl)])


async def delete(key: str):
    """
    Simula redis.delete(key)
    """
    try:
        return await redis_client.delete(key)
    except Exception as e:
        logger.error(f"Redis DELETE error for {key}: {e}")
        return None


async def get(key: str) -> Optional[str]:
    """
    Simula redis.get(key)
    Devuelve siempre un string real o None.
    """
    return await _get(key)


# ============================================================
# 🔥 API pública usada por tus services
# ============================================================

async def cache_set_json(key: str, data: Any, ttl: int = 60):
    """Tus servicios llaman a esto."""
    data_json = json.dumps(data)
    return await setex(key, ttl, data_json)


async def cache_get_json(key: str):
    val = await get(key)
    if val is None:
        return None

    try:
        return json.loads(val)
    except Exception:
        return None


async def cache_delete(key: str):
    return await delete(key)

