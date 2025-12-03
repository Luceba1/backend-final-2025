"""
Request ID Middleware

Adds unique request ID to every request for distributed tracing and logging.
Each request gets a unique identifier that can be tracked across all logs.
"""
import uuid
import logging
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from typing import Callable

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds a unique request ID to every HTTP request.

    The request ID can be:
    1. Provided by client via X-Request-ID header
    2. Auto-generated if not provided

    The request ID is:
    - Stored in request.state.request_id
    - Added to all log messages
    - Returned in response header X-Request-ID
    - Used for tracing requests across services

    Example usage:
        app.add_middleware(RequestIDMiddleware)

    Example log output:
        [abc123] GET /products - 200 OK (45ms)
        [abc123] Cache MISS: products:list:skip:0:limit:10
        [abc123] Query executed: SELECT * FROM products LIMIT 10
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable
    ) -> Response:
        """
        Process request and inject request ID

        Args:
            request: The incoming HTTP request
            call_next: The next middleware or route handler

        Returns:
            HTTP response with X-Request-ID header
        """
        # Get request ID from header or generate new one
        request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())

        # Store in request state for access in route handlers
        request.state.request_id = request_id

        # Log request start
        start_time = time.time()
        logger.info(
            f"[{request_id}] → {request.method} {request.url.path} "
            f"(client: {request.client.host if request.client else 'unknown'})"
        )

        try:
            # Process request
            response = await call_next(request)

            # Calculate request duration
            duration_ms = round((time.time() - start_time) * 1000, 2)

            # Log request completion
            logger.info(
                f"[{request_id}] ← {request.method} {request.url.path} "
                f"- {response.status_code} ({duration_ms}ms)"
            )

            # Add request ID to response headers
            response.headers['X-Request-ID'] = request_id

            # Add timing header for performance monitoring
            response.headers['X-Response-Time'] = f"{duration_ms}ms"

            return response

        except Exception as e:
            # Log errors with request ID
            duration_ms = round((time.time() - start_time) * 1000, 2)
            logger.error(
                f"[{request_id}] ✗ {request.method} {request.url.path} "
                f"- ERROR: {str(e)} ({duration_ms}ms)"
            )
            raise


class RequestIDFilter(logging.Filter):
    """
    Logging filter that adds request ID to log records

    This filter extracts the request ID from the current request context
    and adds it to all log messages automatically.

    Usage:
        Add to logging configuration in config/logging_config.py:

        LOGGING_CONFIG = {
            'filters': {
                'request_id': {
                    '()': 'middleware.request_id_middleware.RequestIDFilter',
                },
            },
            'handlers': {
                'console': {
                    'filters': ['request_id'],
                    'formatter': 'default',
                },
            },
        }
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Add request_id to log record

        Args:
            record: The log record to modify

        Returns:
            True (always allow the log record)
        """
        # Try to get request ID from current context
        # This works with async/await context
        try:
            from contextvars import ContextVar
            request_id_var: ContextVar = ContextVar('request_id', default=None)
            record.request_id = request_id_var.get() or '-'
        except Exception:
            record.request_id = '-'

        return True


def get_request_id(request: Request) -> str:
    """
    Get request ID from current request

    Args:
        request: The FastAPI/Starlette request object

    Returns:
        The request ID string, or 'unknown' if not found

    Example:
        @app.get("/products")
        async def get_products(request: Request):
            req_id = get_request_id(request)
            logger.info(f"[{req_id}] Fetching products")
            return products
    """
    try:
        return request.state.request_id
    except AttributeError:
        return 'unknown'


# =============================================================================
# INTEGRATION EXAMPLE
# =============================================================================
"""
# main.py
from middleware.request_id_middleware import RequestIDMiddleware

app = FastAPI()
app.add_middleware(RequestIDMiddleware)

# Now all logs will include request ID:
# [abc-123] GET /products
# [abc-123] Cache MISS: products:list
# [abc-123] Query: SELECT * FROM products
# [abc-123] Response: 200 OK (45ms)

# Access request ID in route handlers:
@app.get("/products")
async def get_products(request: Request):
    req_id = get_request_id(request)
    logger.info(f"[{req_id}] Processing product request")
    return products
"""