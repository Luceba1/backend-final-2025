"""Address controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.address_schema import AddressSchema
from services.address_service import AddressService


class AddressController(BaseControllerImpl):
    """Controller for Address entity with CRUD operations."""

    def __init__(self):
        super().__init__(
            schema=AddressSchema,
            service_factory=lambda db: AddressService(db),
            tags=["Addresses"]
        )