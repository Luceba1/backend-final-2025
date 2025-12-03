from sqlalchemy import Column, Float, ForeignKey, Integer
from sqlalchemy.orm import relationship

from models.base_model import BaseModel


class OrderDetailModel(BaseModel):
    __tablename__ = "order_details"

    quantity = Column(Integer)
    price = Column(Float)
    order_id = Column(Integer, ForeignKey("orders.id_key"), index=True)
    product_id = Column(Integer, ForeignKey("products.id_key"), index=True)

    order = relationship("OrderModel", back_populates="order_details", lazy="select")
    product = relationship("ProductModel", back_populates="order_details", lazy="select")
