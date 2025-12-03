"""Bill repository for database operations."""
from sqlalchemy.orm import Session

from models.bill import BillModel
from repositories.base_repository_impl import BaseRepositoryImpl
from schemas.bill_schema import BillSchema


class BillRepository(BaseRepositoryImpl):
    """Repository for Bill entity database operations."""

    def __init__(self, db: Session):
        super().__init__(BillModel, BillSchema, db)