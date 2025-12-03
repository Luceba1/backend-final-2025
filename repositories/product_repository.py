"""Product repository for database operations."""
from sqlalchemy.orm import Session

from models.product import ProductModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.product_schema import ProductSchema


class ProductRepository(BaseRepositoryImpl):
    """Repository for Product entity database operations."""

    def __init__(self, db: Session):
        super().__init__(ProductModel, ProductSchema, db)