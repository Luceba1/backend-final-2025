"""Product review schema with validation"""
from typing import Optional
from pydantic import Field

from schemas.base_schema import BaseSchema


class ReviewSchema(BaseSchema):
    """Product review schema with validation"""

    rating: float = Field(
        ...,
        ge=1.0,
        le=5.0,
        description="Rating from 1 to 5 stars (required)"
    )

    comment: Optional[str] = Field(
        None,
        min_length=10,
        max_length=1000,
        description="Review comment (optional, 10-1000 characters)"
    )

    product_id: Optional[int] = Field(default=None)

    # 👇 Importante: NO incluimos ProductSchema acá para evitar bucles Product ↔ Review
    # Si hace falta mostrar info del producto, se hace en un endpoint específico.
