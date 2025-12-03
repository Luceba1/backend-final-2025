from sqlalchemy import Column, String, Float, Date, Enum, Integer, ForeignKey
from sqlalchemy.orm import relationship

from models.base_model import BaseModel
from models.enums import PaymentType


class BillModel(BaseModel):
    __tablename__ = "bills"

    bill_number = Column(String, unique=True, index=True, nullable=False)
    discount = Column(Float)
    date = Column(Date)
    total = Column(Float)
    payment_type = Column(Enum(PaymentType))
    client_id = Column(Integer, ForeignKey('clients.id_key'), index=True)  # ✅ Added

    # Relationships
    order = relationship('OrderModel', back_populates='bill', uselist=False, lazy="select")
    client = relationship('ClientModel', back_populates='bills', lazy="select")  # ✅ Added
