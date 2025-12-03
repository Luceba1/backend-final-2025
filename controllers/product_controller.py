"""Product controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.product_schema import ProductSchema
from services.product_service import ProductService


class ProductController(BaseControllerImpl):
    """Controller for Product entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=ProductSchema,
            service_factory=lambda db: ProductService(db),
            tags=["Products"]
        )