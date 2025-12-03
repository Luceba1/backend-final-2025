"""Module for the ReviewModel class."""

from sqlalchemy import Column, String, Float, Integer, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship

from models.base_model import BaseModel


class ReviewModel(BaseModel):
    """ReviewModel class with attributes and relationships."""

    __tablename__ = 'reviews'

    __table_args__ = (
        CheckConstraint('rating >= 1.0 AND rating <= 5.0', name='check_rating_range'),
    )

    rating = Column(Float, nullable=False)
    comment = Column(String)
    product_id = Column(Integer, ForeignKey('products.id_key'), index=True)
    product = relationship(
        'ProductModel', back_populates='reviews', lazy='select',
        )
