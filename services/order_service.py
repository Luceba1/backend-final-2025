"""OrderService with foreign key validation and business logic."""
import logging
from sqlalchemy.orm import Session
from datetime import datetime

from models.order import OrderModel
from repositories.order_repository import OrderRepository
from repositories.client_repository import ClientRepository
from repositories.bill_repository import BillRepository
from repositories.base_repository_impl import InstanceNotFoundError
from schemas.order_schema import OrderSchema
from services.base_service_impl import BaseServiceImpl
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class OrderService(BaseServiceImpl):
    """Service for Order entity with validation and business logic."""

    def __init__(self, db: Session):
        super().__init__(
            repository_class=OrderRepository,
            model=OrderModel,
            schema=OrderSchema,
            db=db
        )
        self._client_repository = ClientRepository(db)
        self._bill_repository = BillRepository(db)

    def save(self, schema: OrderSchema) -> OrderSchema:
        """
        Create a new order with validation

        Args:
            schema: Order data to create

        Returns:
            Created order

        Raises:
            InstanceNotFoundError: If client or bill doesn't exist
            ValueError: If validation fails
        """
        # Validate client exists
        try:
            self._client_repository.find(schema.client_id)
        except InstanceNotFoundError:
            logger.error(f"Client with id {schema.client_id} not found")
            raise InstanceNotFoundError(f"Client with id {schema.client_id} not found")

        # Validate bill exists
        try:
            self._bill_repository.find(schema.bill_id)
        except InstanceNotFoundError:
            logger.error(f"Bill with id {schema.bill_id} not found")
            raise InstanceNotFoundError(f"Bill with id {schema.bill_id} not found")

        # Set creation date if not provided
        if schema.date is None:
            schema.date = datetime.utcnow()

        # Create order
        logger.info(f"Creating order for client {schema.client_id}")
        return super().save(schema)

    def update(self, id_key: int, schema: OrderSchema) -> OrderSchema:
        """
        Update an order with validation

        Args:
            id_key: Order ID
            schema: Updated order data

        Returns:
            Updated order

        Raises:
            InstanceNotFoundError: If order, client, or bill doesn't exist
        """
        # Validate client exists if being updated
        if schema.client_id is not None:
            try:
                self._client_repository.find(schema.client_id)
            except InstanceNotFoundError:
                logger.error(f"Client with id {schema.client_id} not found")
                raise InstanceNotFoundError(f"Client with id {schema.client_id} not found")

        # Validate bill exists if being updated
        if schema.bill_id is not None:
            try:
                self._bill_repository.find(schema.bill_id)
            except InstanceNotFoundError:
                logger.error(f"Bill with id {schema.bill_id} not found")
                raise InstanceNotFoundError(f"Bill with id {schema.bill_id} not found")

        logger.info(f"Updating order {id_key}")
        return super().update(id_key, schema)
