"""
Health Check Controller with Threshold-Based Monitoring

Provides comprehensive health check including database, Redis,
connection pool status, and threshold-based warnings.

Thresholds:
- DB Pool Utilization: Warning at 70%, Critical at 90%
- DB Latency: Warning at 100ms, Critical at 500ms
- Redis: Upstash async (real status + latency)
"""
import time
from fastapi import APIRouter
from config.database import check_connection, engine
from datetime import datetime

# ✔ Nuevo import correcto (async cache)
from services.cache_service import cache_service

router = APIRouter()

# Health check thresholds
THRESHOLDS = {
    "db_pool_utilization": {
        "warning": 70.0,
        "critical": 90.0
    },
    "db_latency": {
        "warning": 100.0,
        "critical": 500.0
    }
}


def evaluate_health_level(*statuses):
    if "critical" in statuses:
        return "critical"
    if "degraded" in statuses or "down" in statuses:
        return "degraded"
    if "warning" in statuses:
        return "warning"
    return "healthy"


@router.get("/")
async def health_check():
    """
    Comprehensive health check including:
    - Database latency
    - Redis async status (Upstash)
    - DB Pool metrics
    """
    checks = {}
    component_statuses = []

    # ------------------------------------------
    # ✔ DATABASE CHECK (latency + thresholds)
    # ------------------------------------------
    start = time.time()
    db_status = check_connection()
    db_latency_ms = round((time.time() - start) * 1000, 2)

    if not db_status:
        db_health = "critical"
        component_statuses.append("critical")
    elif db_latency_ms >= THRESHOLDS["db_latency"]["critical"]:
        db_health = "critical"
        component_statuses.append("critical")
    elif db_latency_ms >= THRESHOLDS["db_latency"]["warning"]:
        db_health = "warning"
        component_statuses.append("warning")
    else:
        db_health = "healthy"
        component_statuses.append("healthy")

    checks["database"] = {
        "status": "up" if db_status else "down",
        "health": db_health,
        "latency_ms": db_latency_ms if db_status else None,
        "thresholds": THRESHOLDS["db_latency"]
    }

    # ------------------------------------------
    # ✔ REDIS ASYNC CHECK (Upstash + latency)
    # ------------------------------------------
    if not cache_service.is_available():
        redis_health = "degraded"
        redis_status = False
        redis_latency = None
    else:
        try:
            r_start = time.perf_counter()
            pong = await cache_service.redis_client.ping()
            redis_latency = round((time.perf_counter() - r_start) * 1000, 2)
            redis_status = True
            redis_health = "healthy"
        except Exception:
            redis_status = False
            redis_health = "degraded"
            redis_latency = None

    component_statuses.append(redis_health)

    checks["redis"] = {
        "status": "up" if redis_status else "down",
        "health": redis_health,
        "latency_ms": redis_latency,
        "provider": "Upstash",
        "tls": True
    }

    # ------------------------------------------
    # ✔ DB POOL METRICS (SQLAlchemy)
    # ------------------------------------------
    try:
        pool = engine.pool
        total_connections = pool.size() + pool.overflow()
        checked_out = pool.checkedout()
        utilization = (checked_out / total_connections * 100) if total_connections > 0 else 0

        if utilization >= THRESHOLDS["db_pool_utilization"]["critical"]:
            pool_health = "critical"
            component_statuses.append("critical")
        elif utilization >= THRESHOLDS["db_pool_utilization"]["warning"]:
            pool_health = "warning"
            component_statuses.append("warning")
        else:
            pool_health = "healthy"
            component_statuses.append("healthy")

        checks["db_pool"] = {
            "health": pool_health,
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": checked_out,
            "overflow": pool.overflow(),
            "total_capacity": total_connections,
            "utilization_percent": round(utilization, 1),
            "thresholds": THRESHOLDS["db_pool_utilization"]
        }
    except Exception as e:
        checks["db_pool"] = {
            "status": "error",
            "health": "critical",
            "error": str(e)
        }
        component_statuses.append("critical")

    # ------------------------------------------
    # ✔ OVERALL STATUS
    # ------------------------------------------
    overall_status = evaluate_health_level(*component_statuses)

    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "checks": checks
    }
