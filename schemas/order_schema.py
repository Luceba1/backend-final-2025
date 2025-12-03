"""Order schema with validation."""
from datetime import datetime
from typing import Optional
from pydantic import Field

from schemas.base_schema import BaseSchema
from models.enums import DeliveryMethod, Status


class OrderSchema(BaseSchema):
    """Schema for Order entity with validations."""

    date: datetime = Field(default_factory=datetime.utcnow, description="Order date")
    total: float = Field(..., ge=0, description="Total amount (must be >= 0, required)")
    delivery_method: DeliveryMethod = Field(..., description="Delivery method (required)")
    status: Status = Field(default=Status.PENDING, description="Order status")
    client_id: int = Field(..., description="Client ID reference (required)")
    bill_id: int = Field(..., description="Bill ID reference (required)")
