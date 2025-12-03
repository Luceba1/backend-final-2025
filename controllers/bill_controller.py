"""Bill controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.bill_schema import BillSchema
from services.bill_service import BillService


class BillController(BaseControllerImpl):
    """Controller for Bill entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=BillSchema,
            service_factory=lambda db: BillService(db),
            tags=["Bills"]
        )