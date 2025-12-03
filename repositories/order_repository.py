"""Order repository for database operations."""
from sqlalchemy.orm import Session

from models.order import OrderModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.order_schema import OrderSchema


class OrderRepository(BaseRepositoryImpl):
    """Repository for Order entity database operations."""

    def __init__(self, db: Session):
        super().__init__(OrderModel, OrderSchema, db)