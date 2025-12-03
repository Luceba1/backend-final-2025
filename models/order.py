from sqlalchemy import Column, Float, DateTime, Enum, Integer, ForeignKey
from sqlalchemy.orm import relationship

from models.base_model import BaseModel
from models.enums import DeliveryMethod, Status


class OrderModel(BaseModel):
    __tablename__ = "orders"

    date = Column(DateTime, index=True)
    total = Column(Float)
    delivery_method = Column(Enum(DeliveryMethod), index=True)
    status = Column(Enum(Status), index=True)
    client_id = Column(Integer, ForeignKey('clients.id_key'), index=True)
    bill_id = Column(Integer, ForeignKey('bills.id_key'), index=True)

    order_details = relationship("OrderDetailModel", back_populates="order", cascade="all, delete-orphan",
                                 lazy="select")
    client = relationship("ClientModel", back_populates="orders", lazy="select")
    bill = relationship("BillModel", back_populates="order", lazy="select")
