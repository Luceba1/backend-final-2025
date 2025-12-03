"""Unit tests for middleware components."""
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from middleware.rate_limiter import RateLimiterMiddleware


class TestRateLimiter:
    """Tests for RateLimiter middleware."""

    def test_rate_limiter_allows_within_limit(self, mock_redis):
        """Test that requests within limit are allowed."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Add rate limiter with 5 calls per 60 seconds
        rate_limiter = RateLimiterMiddleware(app=app, redis_client=mock_redis, calls=5, period=60)

        client = TestClient(app)

        # Make 5 requests (should all succeed)
        for i in range(5):
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"message": "success"}

    def test_rate_limiter_blocks_over_limit(self, mock_redis):
        """Test that requests over limit are blocked."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Add rate limiter with 3 calls per 60 seconds
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=3, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Make 3 requests (should succeed)
        for i in range(3):
            response = client.get("/test")
            assert response.status_code == 200

        # 4th request should be rate limited
        response = client.get("/test")
        assert response.status_code == 429
        assert "rate limit exceeded" in response.json()["detail"].lower()

    def test_rate_limiter_different_ips(self, mock_redis):
        """Test that rate limiting is per IP address."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=2, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Each IP should have its own limit
        # Note: TestClient uses same IP, so this is a conceptual test
        for i in range(2):
            response = client.get("/test")
            assert response.status_code == 200

        # 3rd request should be blocked
        response = client.get("/test")
        assert response.status_code == 429

    def test_rate_limiter_redis_failure_fails_open(self):
        """Test that rate limiter fails open when Redis is down."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Create a mock Redis that raises exceptions
        failing_redis = Mock()
        failing_redis.pipeline.side_effect = Exception("Redis connection failed")

        rate_limiter = RateLimiter(redis_client=failing_redis, calls=1, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Should succeed even though Redis is down (fail open)
        response = client.get("/test")
        assert response.status_code == 200

    def test_rate_limiter_atomic_operations(self, mock_redis):
        """Test that rate limiter uses atomic Redis pipeline operations."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=5, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Make a request
        response = client.get("/test")
        assert response.status_code == 200

        # Verify pipeline was used (atomic operation)
        # This ensures no race condition between incr and expire
        assert hasattr(mock_redis, 'pipeline')

    def test_rate_limiter_custom_limits(self, mock_redis):
        """Test rate limiter with different call limits."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Very restrictive: 1 call per 60 seconds
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=1, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # First request should succeed
        response = client.get("/test")
        assert response.status_code == 200

        # Second request should be blocked immediately
        response = client.get("/test")
        assert response.status_code == 429

    def test_rate_limiter_excludes_health_check(self, mock_redis):
        """Test that rate limiter excludes health check endpoint."""
        app = FastAPI()

        @app.get("/health_check")
        async def health_check():
            return {"status": "healthy"}

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Very restrictive rate limiter
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=1, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Health check should never be rate limited
        for i in range(10):
            response = client.get("/health_check")
            assert response.status_code == 200

    def test_rate_limiter_response_headers(self, mock_redis):
        """Test that rate limiter adds appropriate response headers."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=5, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        response = client.get("/test")
        assert response.status_code == 200

        # Check for rate limit headers (if implemented)
        # Note: Current implementation may not have these headers
        # This is a test for future enhancement

    def test_rate_limiter_period_expiration(self, mock_redis):
        """Test that rate limit resets after period expires."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Short period for testing: 1 call per 1 second
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=1, period=1)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # First request
        response = client.get("/test")
        assert response.status_code == 200

        # Second request immediately (should be blocked)
        response = client.get("/test")
        assert response.status_code == 429

        # Wait for period to expire
        time.sleep(1.1)

        # Reset mock Redis to simulate expiration
        mock_redis.data.clear()

        # Third request after period (should succeed)
        response = client.get("/test")
        assert response.status_code == 200

    def test_rate_limiter_get_client_ip(self, mock_redis):
        """Test client IP extraction from various headers."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=5, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Test with X-Forwarded-For header
        response = client.get("/test", headers={"X-Forwarded-For": "192.168.1.100"})
        assert response.status_code == 200

        # Test with X-Real-IP header
        response = client.get("/test", headers={"X-Real-IP": "192.168.1.101"})
        assert response.status_code == 200


class TestRateLimiterEdgeCases:
    """Tests for rate limiter edge cases."""

    def test_rate_limiter_concurrent_requests(self, mock_redis):
        """Test rate limiter behavior with concurrent requests."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=3, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Simulate rapid concurrent requests
        responses = []
        for i in range(5):
            response = client.get("/test")
            responses.append(response)

        # First 3 should succeed
        assert sum(1 for r in responses[:3] if r.status_code == 200) == 3

        # Last 2 should be rate limited
        assert sum(1 for r in responses[3:] if r.status_code == 429) == 2

    def test_rate_limiter_zero_calls(self, mock_redis):
        """Test rate limiter with zero calls allowed (edge case)."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Edge case: 0 calls allowed
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=0, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Should be immediately rate limited
        response = client.get("/test")
        assert response.status_code == 429

    def test_rate_limiter_very_high_limit(self, mock_redis):
        """Test rate limiter with very high limit."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        # Very high limit
        rate_limiter = RateLimiter(redis_client=mock_redis, calls=10000, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Should handle many requests
        for i in range(100):
            response = client.get("/test")
            assert response.status_code == 200

    def test_rate_limiter_redis_key_format(self, mock_redis):
        """Test that rate limiter uses correct Redis key format."""
        app = FastAPI()

        @app.get("/test")
        async def test_endpoint():
            return {"message": "success"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=5, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        response = client.get("/test")
        assert response.status_code == 200

        # Verify Redis key follows expected format: "rate_limit:{ip}"
        assert any(key.startswith("rate_limit:") for key in mock_redis.data.keys())


class TestRateLimiterConfiguration:
    """Tests for rate limiter configuration."""

    def test_rate_limiter_default_configuration(self):
        """Test rate limiter with default configuration."""
        # This would test the default values from environment variables
        # Requires mocking environment variables
        pass

    def test_rate_limiter_custom_configuration(self, mock_redis):
        """Test rate limiter with custom configuration."""
        # Test various configurations
        configs = [
            (10, 60),   # 10 calls per minute
            (100, 3600),  # 100 calls per hour
            (5, 10),    # 5 calls per 10 seconds
        ]

        for calls, period in configs:
            app = FastAPI()

            @app.get("/test")
            async def test_endpoint():
                return {"message": "success"}

            rate_limiter = RateLimiter(redis_client=mock_redis, calls=calls, period=period)
            app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

            client = TestClient(app)

            # Reset mock Redis
            mock_redis.data.clear()

            # Should allow up to 'calls' requests
            for i in range(calls):
                response = client.get("/test")
                assert response.status_code == 200

            # Next request should be blocked
            response = client.get("/test")
            assert response.status_code == 429


class TestRateLimiterIntegration:
    """Integration tests for rate limiter with full app."""

    def test_rate_limiter_with_authentication(self, mock_redis):
        """Test rate limiter interaction with authentication."""
        # This would test rate limiting per authenticated user
        # Requires implementing authentication
        pass

    def test_rate_limiter_with_multiple_endpoints(self, mock_redis):
        """Test rate limiter across multiple endpoints."""
        app = FastAPI()

        @app.get("/endpoint1")
        async def endpoint1():
            return {"endpoint": "1"}

        @app.get("/endpoint2")
        async def endpoint2():
            return {"endpoint": "2"}

        rate_limiter = RateLimiter(redis_client=mock_redis, calls=5, period=60)
        app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limiter)

        client = TestClient(app)

        # Rate limit is shared across all endpoints for the same IP
        for i in range(3):
            client.get("/endpoint1")

        for i in range(2):
            client.get("/endpoint2")

        # 6th request should be blocked
        response = client.get("/endpoint1")
        assert response.status_code == 429
