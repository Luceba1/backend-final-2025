"""Category repository for database operations."""
from sqlalchemy.orm import Session

from models.category import CategoryModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.category_schema import CategorySchema


class CategoryRepository(BaseRepositoryImpl):
    """Repository for Category entity database operations."""

    def __init__(self, db: Session):
        super().__init__(CategoryModel, CategorySchema, db)