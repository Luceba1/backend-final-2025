"""OrderDetail repository for database operations."""
from sqlalchemy.orm import Session

from models.order_detail import OrderDetailModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.order_detail_schema import OrderDetailSchema


class OrderDetailRepository(BaseRepositoryImpl):
    """Repository for OrderDetail entity database operations."""

    def __init__(self, db: Session):
        super().__init__(OrderDetailModel, OrderDetailSchema, db)