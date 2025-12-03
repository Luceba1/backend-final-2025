"""OrderDetail controller with proper dependency injection and rate limiting."""
from fastapi import Depends, Request, status
from sqlalchemy.orm import Session
from typing import List

from controllers.base_controller_impl import BaseControllerImpl
from schemas.order_detail_schema import OrderDetailSchema
from services.order_detail_service import OrderDetailService
from config.database import get_db
from middleware.endpoint_rate_limiter import order_rate_limit


class OrderDetailController(BaseControllerImpl):
    """
    Controller for OrderDetail entity with CRUD operations.

    Includes endpoint-specific rate limiting to prevent order spam:
    - POST /order_details: Limited to 10 requests per minute per IP
    """

    def __init__(self):
        super().__init__(
            schema=OrderDetailSchema,
            service_factory=lambda db: OrderDetailService(db),
            tags=["Order Details"]
        )

        # Override POST endpoint with rate limiting
        @self.router.post(
            "/",
            response_model=OrderDetailSchema,
            status_code=status.HTTP_201_CREATED,
            summary="Create Order Detail (Rate Limited)",
            description="Create a new order detail. Limited to 10 requests per minute per IP to prevent spam."
        )
        @order_rate_limit
        async def create_with_rate_limit(
            request: Request,
            schema_in: OrderDetailSchema,
            db: Session = Depends(get_db)
        ):
            """
            Create a new order detail with rate limiting.

            This endpoint is rate-limited to 10 requests per minute per IP address
            to prevent order spam and abuse.
            """
            service = self.service_factory(db)
            return service.save(schema_in)