"""
Main application module for FastAPI e-commerce REST API.
"""
import os
import uvicorn
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette import status
from starlette.responses import JSONResponse

from config.logging_config import setup_logging
from config.database import create_tables, engine
from config.redis_config import check_redis_connection
from middleware.request_id_middleware import RequestIDMiddleware

# Setup logging
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


def create_fastapi_app() -> FastAPI:

    fastapi_app = FastAPI(
        title="E-commerce REST API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )

    @fastapi_app.exception_handler(InstanceNotFoundError)
    async def instance_not_found_handler(request, exc):
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

    # Middleware
    fastapi_app.add_middleware(RequestIDMiddleware)
    logger.info("✅ Request ID middleware enabled")

    # -----------------------------------------
    # ✔️ CORS definitivo — acepta TODO *.vercel.app
    # -----------------------------------------
    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"https://.*\.vercel\.app",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    logger.info("✅ CORS enabled for *.vercel.app (wildcard)")

    @fastapi_app.on_event("startup")
    async def startup_event():
        logger.info("🚀 Starting FastAPI E-commerce API...")

        if check_redis_connection():
            logger.info("✅ Redis cache available")
        else:
            logger.warning("⚠️ Redis NOT available")

    @fastapi_app.on_event("shutdown")
    async def shutdown_event():
        logger.info("👋 Shutting down API...")
        try:
            engine.dispose()
            logger.info("✅ Database engine disposed")
        except Exception as e:
            logger.error(f"❌ DB engine shutdown error: {e}")

    return fastapi_app


app = create_fastapi_app()


def run_app(fastapi_app: FastAPI):
    uvicorn.run(fastapi_app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))


if __name__ == "__main__":
    create_tables()
    run_app(app)

