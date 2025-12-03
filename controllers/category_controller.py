"""Category controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.category_schema import CategorySchema
from services.category_service import CategoryService


class CategoryController(BaseControllerImpl):
    """Controller for Category entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=CategorySchema,
            service_factory=lambda db: CategoryService(db),
            tags=["Categories"]
        )