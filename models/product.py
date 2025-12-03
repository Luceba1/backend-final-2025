"""
SqlAlchemy model for products.

This module defines the ProductModel class which represents a product in the database.
"""

from sqlalchemy import Column, Float, ForeignKey, Integer, String, CheckConstraint
from sqlalchemy.orm import relationship

from models.base_model import BaseModel


class ProductModel(BaseModel):
    """
    Class representing a product in the database.

    This class represents a product. It inherits from BaseModel and adds additional fields
    specific to products: name, price, stock, and category_id. It also defines relationships with
    CategoryModel, ReviewModel, and OrderDetailModel.

    Database constraints:
        - stock must be >= 0 (enforced at DB level)
        - price must be > 0 (enforced by Pydantic validation)
    """

    __tablename__ = 'products'

    # Table-level constraints
    __table_args__ = (
        CheckConstraint('stock >= 0', name='check_product_stock_non_negative'),
    )

    name = Column(String, index=True)
    price = Column(Float, index=True)
    stock = Column(Integer, default=0, nullable=False, index=True)  # ✅ Added index
    category_id = Column(Integer, ForeignKey('categories.id_key'), index=True)

    category = relationship(
        'CategoryModel',
        back_populates='products',
        lazy='select',
    )
    reviews = relationship(
        'ReviewModel',
        back_populates='product',
        cascade='all, delete-orphan',
        lazy='select',
    )
    order_details = relationship(
        'OrderDetailModel',
        back_populates='product',
        cascade='all, delete-orphan',
        lazy='select',
    )
