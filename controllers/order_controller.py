"""Order controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.order_schema import OrderSchema
from services.order_service import OrderService


class OrderController(BaseControllerImpl):
    """Controller for Order entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=OrderSchema,
            service_factory=lambda db: OrderService(db),
            tags=["Orders"]
        )