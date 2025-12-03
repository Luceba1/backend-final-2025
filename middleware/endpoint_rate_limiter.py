"""
Endpoint-Specific Rate Limiter

Provides decorators for applying custom rate limits to specific endpoints.
While global rate limiting protects the entire API, endpoint-specific limits
protect expensive or abuse-prone operations.
"""
import logging
import functools
from typing import Callable
from fastapi import Request, HTTPException, status
from config.redis_config import get_redis_client

logger = logging.getLogger(__name__)


class EndpointRateLimiter:
    """
    Decorator for endpoint-specific rate limiting

    Usage:
        @app.post("/order_details")
        @EndpointRateLimiter(calls=10, period=60)
        async def create_order(data: OrderSchema):
            ...

    This allows 10 order creations per 60 seconds per IP address.
    """

    def __init__(self, calls: int, period: int):
        """
        Initialize rate limiter

        Args:
            calls: Maximum number of calls allowed
            period: Time period in seconds
        """
        self.calls = calls
        self.period = period
        self.redis_client = get_redis_client()

    def __call__(self, func: Callable) -> Callable:
        """
        Decorator implementation

        Args:
            func: The function to rate limit

        Returns:
            Wrapped function with rate limiting
        """
        @functools.wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            # Get client IP
            client_ip = request.client.host if request.client else "unknown"

            # Build rate limit key for this endpoint
            endpoint_path = request.url.path
            key = f"rate_limit:endpoint:{endpoint_path}:{client_ip}"

            # Check if Redis is available
            if self.redis_client is None:
                logger.warning("Redis not available - endpoint rate limiting disabled")
                return await func(request, *args, **kwargs)

            try:
                # Get current request count
                current = self.redis_client.get(key)

                if current is None:
                    # First request in this period
                    pipe = self.redis_client.pipeline()
                    pipe.set(key, 1)
                    pipe.expire(key, self.period)
                    pipe.execute()
                    remaining = self.calls - 1
                else:
                    current = int(current)

                    if current >= self.calls:
                        # Rate limit exceeded
                        ttl = self.redis_client.ttl(key)
                        logger.warning(
                            f"Endpoint rate limit exceeded for {client_ip} "
                            f"on {endpoint_path}: {current}/{self.calls}"
                        )
                        raise HTTPException(
                            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                            detail=f"Rate limit exceeded for this endpoint. "
                                   f"Maximum {self.calls} requests per {self.period} seconds. "
                                   f"Try again in {ttl} seconds.",
                            headers={
                                "X-RateLimit-Limit": str(self.calls),
                                "X-RateLimit-Remaining": "0",
                                "X-RateLimit-Reset": str(ttl),
                                "Retry-After": str(ttl),
                            }
                        )

                    # Increment counter
                    self.redis_client.incr(key)
                    remaining = self.calls - current - 1

                # Execute the endpoint
                logger.debug(
                    f"Endpoint rate limit check passed for {client_ip} "
                    f"on {endpoint_path}: {remaining} remaining"
                )
                return await func(request, *args, **kwargs)

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error in endpoint rate limiting: {e}")
                # Fail open - allow request if rate limiting fails
                return await func(request, *args, **kwargs)

        return wrapper


# =============================================================================
# PRESET RATE LIMITERS FOR COMMON USE CASES
# =============================================================================

# Strict limit for order creation (prevents spam orders)
order_rate_limit = EndpointRateLimiter(calls=10, period=60)  # 10 orders/min

# Moderate limit for client creation (prevents account spam)
client_rate_limit = EndpointRateLimiter(calls=5, period=60)  # 5 clients/min

# Strict limit for reviews (prevents review bombing)
review_rate_limit = EndpointRateLimiter(calls=3, period=60)  # 3 reviews/min

# Generous limit for searches (allow frequent searches)
search_rate_limit = EndpointRateLimiter(calls=30, period=60)  # 30 searches/min


# =============================================================================
# USAGE EXAMPLES
# =============================================================================
"""
# Example 1: Protect order creation endpoint
from middleware.endpoint_rate_limiter import order_rate_limit

@router.post("/")
@order_rate_limit
async def create_order(
    request: Request,
    order: OrderSchema,
    db: Session = Depends(get_db)
):
    service = OrderService(db)
    return service.save(order)


# Example 2: Custom rate limit
from middleware.endpoint_rate_limiter import EndpointRateLimiter

custom_limiter = EndpointRateLimiter(calls=20, period=300)  # 20 per 5 minutes

@router.post("/expensive-operation")
@custom_limiter
async def expensive_operation(request: Request):
    # Expensive computation here
    return result


# Example 3: Multiple rate limiters
# Apply both global (via middleware) and endpoint-specific limits

# Global: 100 requests/min to entire API
# Endpoint: 10 order creations/min

# If user hits 100 requests/min globally → 429 from middleware
# If user hits 10 order creations/min → 429 from endpoint limiter
"""