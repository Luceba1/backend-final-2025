"""Client controller with proper dependency injection."""
from controllers.base_controller_impl import BaseControllerImpl
from schemas.client_schema import ClientSchema
from services.client_service import ClientService


class ClientController(BaseControllerImpl):
    """Controller for Client entity with CRUD operations."""

    def __init__(self):
        """
        Initialize ClientController with dependency injection.

        The service is created per request with the database session.
        """
        super().__init__(
            schema=ClientSchema,
            service_factory=lambda db: ClientService(db),
            tags=["Clients"]
        )
