"""
Rate Limiting Middleware

Protects the API from abuse by limiting the number of requests
per client IP address using Redis.
"""
import os
import logging
from typing import Callable
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from config.redis_config import get_redis_client

logger = logging.getLogger(__name__)


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware using Redis

    Limits requests per IP address within a time window.
    """

    def __init__(self, app, calls: int = 100, period: int = 60):
        """
        Initialize rate limiter

        Args:
            app: FastAPI application
            calls: Maximum number of requests allowed
            period: Time window in seconds
        """
        super().__init__(app)
        self.calls = int(os.getenv('RATE_LIMIT_CALLS', str(calls)))
        self.period = int(os.getenv('RATE_LIMIT_PERIOD', str(period)))
        self.enabled = os.getenv('RATE_LIMIT_ENABLED', 'true').lower() == 'true'
        self.redis_client = get_redis_client()

        if self.enabled and self.redis_client:
            logger.info(
                f"✅ Rate limiting enabled: {self.calls} requests per "
                f"{self.period} seconds per IP"
            )
        else:
            logger.warning("⚠️  Rate limiting disabled (Redis not available)")

    async def dispatch(self, request: Request, call_next: Callable):
        """
        Process request with rate limiting

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler

        Returns:
            HTTP response
        """
        # Skip if disabled or Redis unavailable
        if not self.enabled or not self.redis_client:
            return await call_next(request)

        # Skip rate limiting for health check endpoint
        if request.url.path == "/health_check":
            return await call_next(request)

        # Get client IP
        client_ip = self._get_client_ip(request)

        # Check rate limit
        if not self._is_allowed(client_ip):
            logger.warning(f"⚠️  Rate limit exceeded for IP: {client_ip}")
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": f"Rate limit exceeded. Maximum {self.calls} requests "
                              f"per {self.period} seconds.",
                    "retry_after": self.period
                },
                headers={
                    "Retry-After": str(self.period),
                    "X-RateLimit-Limit": str(self.calls),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(self.period)
                }
            )

        # Process request
        response = await call_next(request)

        # Add rate limit headers to response
        remaining = self._get_remaining(client_ip)
        response.headers["X-RateLimit-Limit"] = str(self.calls)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(self.period)

        return response

    def _get_client_ip(self, request: Request) -> str:
        """
        Extract client IP from request

        Args:
            request: HTTP request

        Returns:
            Client IP address
        """
        # Check X-Forwarded-For header (from reverse proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fallback to direct client
        return request.client.host if request.client else "unknown"

    def _is_allowed(self, client_ip: str) -> bool:
        """
        Check if client is allowed to make request with atomic Redis operations

        Args:
            client_ip: Client IP address

        Returns:
            True if allowed, False if rate limit exceeded
        """
        try:
            key = f"rate_limit:{client_ip}"

            # Use pipeline to avoid race condition between incr and expire
            pipe = self.redis_client.pipeline()
            pipe.incr(key)
            pipe.expire(key, self.period)
            results = pipe.execute()

            # ✅ Verify both operations succeeded
            if len(results) < 2:
                logger.error(
                    f"Pipeline returned incomplete results for {client_ip}: "
                    f"expected 2, got {len(results)}"
                )
                return True  # Fail open on error

            current = results[0]  # New counter value
            expire_set = results[1]  # 1 if success, 0 if key doesn't exist

            # ✅ If expire failed, force set expiration
            if not expire_set:
                logger.warning(
                    f"Failed to set expiration for rate limit key: {key}, "
                    f"forcing expiration"
                )
                try:
                    self.redis_client.expire(key, self.period)
                except Exception as exp_error:
                    logger.error(f"Failed to force expiration for {key}: {exp_error}")
                    # Delete key to prevent permanent block
                    try:
                        self.redis_client.delete(key)
                    except Exception:
                        pass

            # Check if limit exceeded
            return current <= self.calls

        except Exception as e:
            logger.error(f"Rate limiting error for {client_ip}: {e}")
            # On error, allow request (fail open)
            return True

    def _get_remaining(self, client_ip: str) -> int:
        """
        Get remaining requests for client

        Args:
            client_ip: Client IP address

        Returns:
            Number of remaining requests
        """
        try:
            key = f"rate_limit:{client_ip}"
            current = self.redis_client.get(key)

            if current is None:
                return self.calls

            remaining = self.calls - int(current)
            return max(0, remaining)

        except Exception as e:
            logger.error(f"Error getting remaining requests for {client_ip}: {e}")
            return self.calls


# Alternative: Decorator-based rate limiter for specific endpoints
class EndpointRateLimiter:
    """
    Decorator for endpoint-specific rate limiting

    Usage:
        rate_limiter = EndpointRateLimiter(calls=10, period=60)

        @app.get("/expensive-endpoint")
        @rate_limiter
        async def expensive_endpoint():
            return {"message": "success"}
    """

    def __init__(self, calls: int = 10, period: int = 60):
        self.calls = calls
        self.period = period
        self.redis_client = get_redis_client()

    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            # Get request from kwargs or args
            request = kwargs.get('request') or next(
                (arg for arg in args if isinstance(arg, Request)), None
            )

            if not request or not self.redis_client:
                return await func(*args, **kwargs)

            client_ip = self._get_client_ip(request)
            key = f"endpoint_rate_limit:{func.__name__}:{client_ip}"

            # Check rate limit using pipeline to avoid race condition
            pipe = self.redis_client.pipeline()
            pipe.incr(key)
            pipe.expire(key, self.period)
            results = pipe.execute()
            current = results[0]

            if current > self.calls:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Endpoint rate limit exceeded. Maximum {self.calls} "
                           f"requests per {self.period} seconds."
                )

            return await func(*args, **kwargs)

        return wrapper

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request"""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"