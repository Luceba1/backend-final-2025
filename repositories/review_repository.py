"""Review repository for database operations."""
from sqlalchemy.orm import Session

from models.review import ReviewModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.review_schema import ReviewSchema

class ReviewRepository(BaseRepositoryImpl):
    """Repository for Review entity database operations."""

    def __init__(self, db: Session):
        super().__init__(ReviewModel, ReviewSchema, db)