"""OrderDetail service with foreign key validation and stock management."""
import logging
from sqlalchemy.orm import Session

from models.order_detail import OrderDetailModel
from models.product import ProductModel
from repositories.order_detail_repository import OrderDetailRepository
from repositories.order_repository import OrderRepository
from repositories.product_repository import ProductRepository
from repositories.base_repository_impl import InstanceNotFoundError
from schemas.order_detail_schema import OrderDetailSchema
from services.base_service_impl import BaseServiceImpl
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class OrderDetailService(BaseServiceImpl):
    """Service for OrderDetail entity with validation and stock management."""

    def __init__(self, db: Session):
        super().__init__(
            repository_class=OrderDetailRepository,
            model=OrderDetailModel,
            schema=OrderDetailSchema,
            db=db
        )
        self._order_repository = OrderRepository(db)
        self._product_repository = ProductRepository(db)

    def save(self, schema: OrderDetailSchema) -> OrderDetailSchema:
        """
        Create a new order detail with validation and atomic stock management

        This method uses pessimistic locking (SELECT FOR UPDATE) to prevent
        race conditions when multiple requests try to purchase the same product
        simultaneously.

        Args:
            schema: Order detail data to create

        Returns:
            Created order detail

        Raises:
            InstanceNotFoundError: If order or product doesn't exist
            ValueError: If stock is insufficient or validation fails
        """
        from sqlalchemy import select

        # Validate order exists
        try:
            self._order_repository.find(schema.order_id)
        except InstanceNotFoundError:
            logger.error(f"Order with id {schema.order_id} not found")
            raise InstanceNotFoundError(f"Order with id {schema.order_id} not found")

        # Use pessimistic locking to prevent race conditions
        # SELECT FOR UPDATE locks the row until transaction completes
        try:
            stmt = select(ProductModel).where(
                ProductModel.id_key == schema.product_id
            ).with_for_update()

            product_model = self._product_repository.session.execute(stmt).scalar_one_or_none()

            if product_model is None:
                logger.error(f"Product with id {schema.product_id} not found")
                raise InstanceNotFoundError(f"Product with id {schema.product_id} not found")

            # Validate stock availability (now with exclusive lock)
            if product_model.stock < schema.quantity:
                logger.error(
                    f"Insufficient stock for product {schema.product_id}: "
                    f"requested {schema.quantity}, available {product_model.stock}"
                )
                raise ValueError(
                    f"Insufficient stock for product {schema.product_id}. "
                    f"Requested: {schema.quantity}, Available: {product_model.stock}"
                )

            # Set price from product if not provided
            if schema.price is None:
                schema.price = product_model.price
                logger.info(f"Using product price: {product_model.price}")

            # Validate price matches product price (prevent price manipulation)
            if abs(schema.price - product_model.price) > 0.01:
                logger.warning(
                    f"Price mismatch for product {schema.product_id}: "
                    f"schema={schema.price}, product={product_model.price}"
                )
                raise ValueError(
                    f"Price mismatch. Expected {product_model.price}, got {schema.price}"
                )

            # Atomically deduct stock and create order detail in same transaction
            product_model.stock -= schema.quantity
            logger.info(
                f"Stock deducted for product {schema.product_id}: "
                f"new stock = {product_model.stock}"
            )

            # Create order detail (same transaction)
            logger.info(f"Creating order detail for order {schema.order_id}")
            result = super().save(schema)

            # Both operations commit together automatically
            # If either fails, both rollback
            logger.info(
                f"Order detail created successfully with atomic stock update"
            )

            return result

        except InstanceNotFoundError:
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error creating order detail: {e}")
            raise

    def update(self, id_key: int, schema: OrderDetailSchema) -> OrderDetailSchema:
        """
        Update an order detail with validation and atomic stock management

        This method uses pessimistic locking (SELECT FOR UPDATE) to prevent
        race conditions when updating quantities concurrently.

        Args:
            id_key: Order detail ID
            schema: Updated order detail data

        Returns:
            Updated order detail

        Raises:
            InstanceNotFoundError: If order detail, order, or product doesn't exist
            ValueError: If validation fails or insufficient stock
        """
        from sqlalchemy import select

        # Get existing order detail to restore stock if quantity changes
        existing = self._repository.find(id_key)

        # Validate order exists if being updated
        if schema.order_id is not None:
            try:
                self._order_repository.find(schema.order_id)
            except InstanceNotFoundError:
                logger.error(f"Order with id {schema.order_id} not found")
                raise InstanceNotFoundError(f"Order with id {schema.order_id} not found")

        # Validate product and handle stock changes with pessimistic locking
        if schema.product_id is not None or schema.quantity is not None:
            product_id = schema.product_id if schema.product_id is not None else existing.product_id

            # 🔒 Use SELECT FOR UPDATE to lock the product row
            try:
                stmt = select(ProductModel).where(
                    ProductModel.id_key == product_id
                ).with_for_update()

                product_model = self._product_repository.session.execute(stmt).scalar_one_or_none()

                if product_model is None:
                    logger.error(f"Product with id {product_id} not found")
                    raise InstanceNotFoundError(f"Product with id {product_id} not found")

                # If quantity is changing, adjust stock atomically
                if schema.quantity is not None and schema.quantity != existing.quantity:
                    quantity_diff = schema.quantity - existing.quantity

                    # Check if we have enough stock for increase (with exclusive lock)
                    if quantity_diff > 0 and product_model.stock < quantity_diff:
                        logger.error(
                            f"Insufficient stock for product {product_id}: "
                            f"requested additional {quantity_diff}, available {product_model.stock}"
                        )
                        raise ValueError(
                            f"Insufficient stock for product {product_id}. "
                            f"Requested additional: {quantity_diff}, Available: {product_model.stock}"
                        )

                    # Update stock atomically (same transaction with lock)
                    product_model.stock -= quantity_diff
                    logger.info(
                        f"Stock adjusted for product {product_id}: "
                        f"change = {-quantity_diff}, new stock = {product_model.stock}"
                    )

            except InstanceNotFoundError:
                raise
            except ValueError:
                raise
            except Exception as e:
                logger.error(f"Error updating stock for product {product_id}: {e}")
                raise

        logger.info(f"Updating order detail {id_key}")
        return super().update(id_key, schema)

    def delete(self, id_key: int) -> None:
        """
        Delete an order detail and restore stock atomically

        This method uses pessimistic locking (SELECT FOR UPDATE) to prevent
        race conditions when restoring stock during concurrent deletes.

        Args:
            id_key: Order detail ID to delete

        Raises:
            InstanceNotFoundError: If order detail or product doesn't exist
        """
        from sqlalchemy import select

        # Get order detail to restore stock
        order_detail = self._repository.find(id_key)

        # 🔒 Use SELECT FOR UPDATE to lock the product row before restoring stock
        try:
            stmt = select(ProductModel).where(
                ProductModel.id_key == order_detail.product_id
            ).with_for_update()

            product_model = self._product_repository.session.execute(stmt).scalar_one_or_none()

            if product_model is None:
                logger.error(f"Product with id {order_detail.product_id} not found")
                raise InstanceNotFoundError(
                    f"Product with id {order_detail.product_id} not found"
                )

            # Restore stock atomically (same transaction with lock)
            product_model.stock += order_detail.quantity

            logger.info(
                f"Stock restored for product {order_detail.product_id}: "
                f"restored {order_detail.quantity}, new stock = {product_model.stock}"
            )

            # Delete order detail (same transaction)
            logger.info(f"Deleting order detail {id_key}")
            super().delete(id_key)

            # Both operations commit together automatically
            # If either fails, both rollback

        except InstanceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error deleting order detail {id_key}: {e}")
            raise