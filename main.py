"""
Main application module for FastAPI e-commerce REST API.

This module initializes the FastAPI application, registers all routers,
and configures global exception handlers.
"""
import os
import uvicorn
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette import status
from starlette.responses import JSONResponse

from config.logging_config import setup_logging
from config.database import create_tables, engine
from config.redis_config import check_redis_connection, redis_client
from middleware.request_id_middleware import RequestIDMiddleware

# Setup centralized logging FIRST
setup_logging()
logger = logging.getLogger(__name__)

# Controllers
from controllers.address_controller import AddressController
from controllers.bill_controller import BillController
from controllers.category_controller import CategoryController
from controllers.client_controller import ClientController
from controllers.order_controller import OrderController
from controllers.order_detail_controller import OrderDetailController
from controllers.product_controller import ProductController
from controllers.review_controller import ReviewController
from controllers.health_check import router as health_check_controller

from repositories.base_repository_impl import InstanceNotFoundError


# ================================================================
# 🔥 Rate Limiter compatible con Upstash REST (sin pipeline, sin ex)
# ================================================================
def rate_limiter_sync(request: Request):
    ip = request.client.host
    key = f"rate_limit:{ip}"

    try:
        current = redis_client.get(key)

        # Primer request → crear clave y setear expiración
        if current is None:
            redis_client.set(key, "1")
            redis_client.expire(key, 60)  # TTL 60 segundos
            return None

        count = int(current)

        # Límite de 100 req/60s
        if count >= 100:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Try again later."}
            )

        # Incrementar + renovar expiración
        redis_client.set(key, str(count + 1))
        redis_client.expire(key, 60)

    except Exception as e:
        logger.error(f"Rate limiter error: {e}")
        # Si redis falla → seguir sin limitar
        return None

    return None


# ================================================================
# 🔥 Crear FastAPI app
# ================================================================
def create_fastapi_app() -> FastAPI:

    fastapi_app = FastAPI(
        title="E-commerce REST API",
        description="FastAPI REST API for e-commerce system with PostgreSQL",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )

    # Global exception handlers
    @fastapi_app.exception_handler(InstanceNotFoundError)
    async def instance_not_found_exception_handler(request, exc):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": str(exc)},
        )

    # Routers
    fastapi_app.include_router(ClientController().router, prefix="/clients")
    fastapi_app.include_router(OrderController().router, prefix="/orders")
    fastapi_app.include_router(ProductController().router, prefix="/products")
    fastapi_app.include_router(AddressController().router, prefix="/addresses")
    fastapi_app.include_router(BillController().router, prefix="/bills")
    fastapi_app.include_router(OrderDetailController().router, prefix="/order_details")
    fastapi_app.include_router(ReviewController().router, prefix="/reviews")
    fastapi_app.include_router(CategoryController().router, prefix="/categories")
    fastapi_app.include_router(health_check_controller, prefix="/health_check")

    # Request ID middleware
    fastapi_app.add_middleware(RequestIDMiddleware)
    logger.info("✅ Request ID middleware enabled")

    # CORS
    vercel_url = os.getenv("FRONTEND_URL", "http://localhost:5500")

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            vercel_url,
            "http://localhost:5500",
            "http://127.0.0.1:5500"
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info(f"✅ CORS enabled for {vercel_url}")

    # =====================================================
    # 🔥 Middleware real — Rate limit Upstash REST compatible
    # =====================================================
    @fastapi_app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        result = rate_limiter_sync(request)
        if isinstance(result, JSONResponse):
            return result
        return await call_next(request)

    logger.info("🔥 Custom Rate Limiting enabled (Upstash REST compatible)")

    # Startup event
    @fastapi_app.on_event("startup")
    async def startup_event():
        logger.info("🚀 Starting FastAPI E-commerce API...")

        if check_redis_connection():
            logger.info("✅ Redis cache available (Upstash REST)")
        else:
            logger.warning("⚠️ Redis not available — running without cache")

    # Shutdown event
    @fastapi_app.on_event("shutdown")
    async def shutdown_event():
        logger.info("👋 Shutting down FastAPI API...")
        try:
            engine.dispose()
            logger.info("✅ Database engine disposed")
        except Exception as e:
            logger.error(f"❌ Error disposing DB engine: {e}")

    return fastapi_app


# ================================================================
# 🔥 Uvicorn local
# ================================================================
def run_app(fastapi_app: FastAPI):
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(fastapi_app, host="0.0.0.0", port=port)


# ================================================================
# 🔥 REQUIRED BY RENDER
# ================================================================
app = create_fastapi_app()

if __name__ == "__main__":
    create_tables()
    run_app(app)

