"""OrderDetail schema with validation."""
from typing import Optional, TYPE_CHECKING
from pydantic import Field

from schemas.base_schema import BaseSchema

if TYPE_CHECKING:
    from schemas.order_schema import OrderSchema
    # OJO: No importamos ProductSchema aquí para evitar ciclos


class OrderDetailSchema(BaseSchema):
    """Schema for OrderDetail entity with validations."""

    quantity: int = Field(
        ...,
        gt=0,
        description="Quantity (required, must be positive)"
    )

    price: Optional[float] = Field(
        None,
        gt=0,
        description="Price (auto-filled from product if not provided)"
    )

    order_id: int = Field(
        ...,
        description="Order ID reference (required)"
    )

    product_id: int = Field(
        ...,
        description="Product ID reference (required)"
    )

    # 🔹 Podemos mantener una referencia ligera al OrderSchema (no genera bucle con Product)
    order: Optional['OrderSchema'] = None

    # ❌ NO incluimos product: ProductSchema para evitar bucle Product ↔ OrderDetail
