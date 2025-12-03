import json
import logging
from typing import Any, Optional

from config.redis_config import redis_client

logger = logging.getLogger(__name__)


# ============================================================
# 🔥 Utilidades internas
# ============================================================

def _decode(result: Any) -> Optional[str]:
    """
    Upstash devuelve:
    { "result": "valor" }
    o None si no existe.

    Esta función devuelve siempre SOLO el valor.
    """
    if result is None:
        return None

    if isinstance(result, dict) and "result" in result:
        return result["result"]

    return result


async def _set_with_ttl(key: str, value: str, ttl: int):
    """
    SET + TTL usando comando REST:
    ["SET", key, value, "EX", ttl]
    """
    try:
        await redis_client.send(["SET", key, value, "EX", str(ttl)])
    except Exception as e:
        logger.error(f"Cache SET error for key '{key}': {e}")


async def _get(key: str) -> Optional[str]:
    try:
        res = await redis_client.get(key)
        return _decode(res)
    except Exception as e:
        logger.error(f"Cache GET error for key '{key}': {e}")
        return None


async def _delete(key: str):
    try:
        await redis_client.delete(key)
    except Exception as e:
        logger.error(f"Cache DELETE error for key '{key}': {e}")


# ============================================================
# 🔥 API pública del servicio de caché
# ============================================================

async def cache_set_json(key: str, data: Any, ttl: int = 60):
    """
    Guarda JSON con TTL usando Upstash REST.
    """
    value = json.dumps(data)
    await _set_with_ttl(key, value, ttl)


async def cache_get_json(key: str) -> Optional[Any]:
    """
    Recupera JSON desde Redis.
    """
    val = await _get(key)
    if val is None:
        return None

    try:
        return json.loads(val)
    except Exception:
        return None


async def cache_set_text(key: str, data: str, ttl: int = 60):
    """Guarda string plano con TTL."""
    await _set_with_ttl(key, data, ttl)


async def cache_get_text(key: str) -> Optional[str]:
    """Obtiene string plano."""
    return await _get(key)


async def cache_delete(key: str):
    """Elimina una clave del caché."""
    await _delete(key)


# ============================================================
# 🔥 Invalidación de listas por modelo
# ============================================================

async def invalidate_list_cache(prefix: str):
    """
    Como Upstash REST **NO soporta KEYS**, no podemos borrar por patrón.

    Solución:
    - Todas las claves de listas deben formarse así:
      f"{prefix}:list:{...}"

    - Guardamos una segunda clave "control" con la lista de keys.
    """
    control_key = f"{prefix}:_keys"

    try:
        raw = await _get(control_key)
        if not raw:
            return

        keys = json.loads(raw)

        for k in keys:
            await _delete(k)

        # borrar control
        await _delete(control_key)

    except Exception as e:
        logger.error(f"Error invalidating cache for prefix '{prefix}': {e}")


async def register_list_key(prefix: str, key: str):
    """
    Registra cada clave creada para poder invalidarla después.
    """
    control_key = f"{prefix}:_keys"

    try:
        raw = await _get(control_key)
        if raw:
            keys = json.loads(raw)
        else:
            keys = []

        if key not in keys:
            keys.append(key)

        await _set_with_ttl(control_key, json.dumps(keys), 3600)

    except Exception as e:
        logger.error(f"Error registering list key '{key}': {e}")
