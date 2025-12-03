from sqlalchemy.orm import Session
from models.bill import BillModel
from repositories.bill_repository import BillRepository
from schemas.bill_schema import BillSchema
from services.base_service_impl import BaseServiceImpl


class BillService(BaseServiceImpl):
    def __init__(self, db: Session):
        super().__init__(
            repository_class=BillRepository,
            model=BillModel,
            schema=BillSchema,
            db=db
        )
