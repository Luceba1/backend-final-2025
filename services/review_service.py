"""Review service with proper dependency injection."""
from sqlalchemy.orm import Session

from models.review import ReviewModel
from repositories.review_repository import ReviewRepository
from schemas.review_schema import ReviewSchema
from services.base_service_impl import BaseServiceImpl


class ReviewService(BaseServiceImpl):
    """Service for Review entity business logic."""

    def __init__(self, db: Session):
        super().__init__(
            repository_class=ReviewRepository,
            model=ReviewModel,
            schema=ReviewSchema,
            db=db
        )
