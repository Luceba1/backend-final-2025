"""
Health Check Controller with Threshold-Based Monitoring

Provides comprehensive health check including database, Redis,
connection pool status, and threshold-based warnings.

Thresholds:
- DB Pool Utilization: Warning at 70%, Critical at 90%
- DB Latency: Warning at 100ms, Critical at 500ms
- Redis: Binary (up/down)
"""
import time
from fastapi import APIRouter
from config.database import check_connection, engine
from config.redis_config import check_redis_connection
from datetime import datetime

router = APIRouter()

# Health check thresholds
THRESHOLDS = {
    "db_pool_utilization": {
        "warning": 70.0,  # %
        "critical": 90.0  # %
    },
    "db_latency": {
        "warning": 100.0,  # milliseconds
        "critical": 500.0  # milliseconds
    }
}


def evaluate_health_level(*statuses):
    """
    Evaluate overall health based on multiple component statuses.

    Priority: critical > degraded > warning > healthy

    Args:
        *statuses: Variable number of status strings

    Returns:
        Overall health status string
    """
    if "critical" in statuses:
        return "critical"
    if "degraded" in statuses or "down" in statuses:
        return "degraded"
    if "warning" in statuses:
        return "warning"
    return "healthy"


@router.get("/")
def health_check():
    """
    Comprehensive health check endpoint with threshold-based monitoring

    Returns the status of:
    - Database connection (with latency thresholds)
    - Redis cache
    - Database connection pool metrics (with utilization thresholds)
    - System timestamp
    - Overall health level (healthy/warning/degraded/critical)

    Status Levels:
    - healthy: All systems operational
    - warning: Performance degradation detected (exceeds warning thresholds)
    - degraded: Non-critical component down (e.g., Redis)
    - critical: Critical component down or exceeds critical thresholds
    """
    checks = {}
    component_statuses = []

    # Database health check with latency thresholds
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
        "thresholds": {
            "warning_ms": THRESHOLDS["db_latency"]["warning"],
            "critical_ms": THRESHOLDS["db_latency"]["critical"]
        }
    }

    # Redis health check
    redis_status = check_redis_connection()
    redis_health = "healthy" if redis_status else "degraded"
    component_statuses.append(redis_health)

    checks["redis"] = {
        "status": "up" if redis_status else "down",
        "health": redis_health
    }

    # Database connection pool metrics with utilization thresholds
    try:
        pool = engine.pool
        total_connections = pool.size() + pool.overflow()
        checked_out = pool.checkedout()
        utilization = (checked_out / total_connections * 100) if total_connections > 0 else 0

        # Evaluate pool health based on utilization
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
            "thresholds": {
                "warning_percent": THRESHOLDS["db_pool_utilization"]["warning"],
                "critical_percent": THRESHOLDS["db_pool_utilization"]["critical"]
            }
        }
    except Exception as e:
        checks["db_pool"] = {
            "status": "error",
            "health": "critical",
            "error": str(e)
        }
        component_statuses.append("critical")

    # Overall status based on all components
    overall_status = evaluate_health_level(*component_statuses)

    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "checks": checks
    }
