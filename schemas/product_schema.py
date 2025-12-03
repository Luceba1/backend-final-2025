# schemas/product_schema.py
from typing import Optional, List, TYPE_CHECKING
from pydantic import Field
from schemas.base_schema import BaseSchema

if TYPE_CHECKING:
    from schemas.category_schema import CategorySchema
    from schemas.order_detail_schema import OrderDetailSchema
    from schemas.review_schema import ReviewSchema


class ProductSchema(BaseSchema):
    name: str = Field(..., min_length=1, max_length=200)
    price: float = Field(..., gt=0)
    stock: int = Field(default=0, ge=0)
    category_id: Optional[int] = Field(default=None)

    category: Optional["CategorySchema"] = None
    reviews: List["ReviewSchema"] = Field(default_factory=list)
    order_details: List["OrderDetailSchema"] = Field(default_factory=list)

