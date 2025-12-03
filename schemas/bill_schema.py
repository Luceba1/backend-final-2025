"""Bill schema with validation."""
from datetime import date as DateType
from typing import Optional, TYPE_CHECKING
from pydantic import Field

from schemas.base_schema import BaseSchema
from models.enums import PaymentType

if TYPE_CHECKING:
    from schemas.order_schema import OrderSchema
    from schemas.client_schema import ClientSchema


class BillSchema(BaseSchema):
    """Schema for Bill entity with validations."""

    bill_number: str = Field(..., min_length=1, max_length=50, description="Unique bill number (required)")
    discount: Optional[float] = Field(None, ge=0, description="Discount amount (must be >= 0)")
    date: DateType = Field(..., description="Bill date (required)")
    total: float = Field(..., ge=0, description="Total amount (must be >= 0, required)")
    payment_type: PaymentType = Field(..., description="Payment type (required)")
    client_id: int = Field(..., description="Client ID reference (required)")  # ✅ Added

    # Relationships
    order: Optional['OrderSchema'] = None
    client: Optional['ClientSchema'] = None  # ✅ Added
