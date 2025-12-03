# schemas/category_schema.py
from pydantic import Field
from typing import List
from schemas.base_schema import BaseSchema

class ProductSummarySchema(BaseSchema):
    name: str = Field(..., min_length=1, max_length=200)

class CategorySchema(BaseSchema):
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Nombre de la categoría (obligatorio)"
    )
    products: List[ProductSummarySchema] = Field(default_factory=list)

