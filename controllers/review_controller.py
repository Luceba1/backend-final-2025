"""Review controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.review_schema import ReviewSchema
from services.review_service import ReviewService


class ReviewController(BaseControllerImpl):
    """Controller for Review entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=ReviewSchema,
            service_factory=lambda db: ReviewService(db),
            tags=["Reviews"]
        )