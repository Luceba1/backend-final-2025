"""
BaseRepository implementation with best practices and sanitized logging
"""
import logging
from typing import Type, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select

from models.base_model import BaseModel
from repositories.base_repository import BaseRepository
from schemas.base_schema import BaseSchema
from utils.logging_utils import log_repository_error, create_user_safe_error, get_sanitized_logger


class InstanceNotFoundError(Exception):
    """
    InstanceNotFoundError is raised when a record is not found
    """
    pass


class BaseRepositoryImpl(BaseRepository):
    """
    Base Repository Implementation with proper error handling and SQLAlchemy 2.0 patterns
    """

    def __init__(self, model: Type[BaseModel], schema: Type[BaseSchema], db: Session):
        self._model = model
        self._schema = schema
        self._session = db
        self.logger = get_sanitized_logger(__name__)  # P11: Sanitized logging

    @property
    def session(self) -> Session:
        """Get the database session"""
        return self._session

    @property
    def model(self) -> Type[BaseModel]:
        """Get the SQLAlchemy model class"""
        return self._model

    @property
    def schema(self) -> Type[BaseSchema]:
        """Get the Pydantic schema class"""
        return self._schema

    def find(self, id_key: int) -> BaseSchema:
        """
        Find a single record by ID

        Args:
            id_key: The primary key value

        Returns:
            The schema instance

        Raises:
            InstanceNotFoundError: If the record is not found
        """
        try:
            # Use SQLAlchemy 2.0 style query
            stmt = select(self.model).where(self.model.id_key == id_key)
            model = self.session.scalars(stmt).first()

            if model is None:
                raise InstanceNotFoundError(
                    f"{self.model.__name__} with id {id_key} not found"
                )

            return self.schema.model_validate(model)
        except InstanceNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"Error finding {self.model.__name__} with id {id_key}: {e}")
            raise

    def find_all(self, skip: int = 0, limit: int = 100) -> List[BaseSchema]:
        """
        Find all records with pagination and input validation

        This method validates pagination parameters to prevent DoS attacks
        and ensure reasonable query performance.

        Args:
            skip: Number of records to skip (must be >= 0)
            limit: Maximum number of records to return (must be 1-1000)

        Returns:
            List of schema instances

        Raises:
            ValueError: If pagination parameters are invalid
        """
        from config.constants import PaginationConfig, ErrorMessages

        try:
            # Validate skip parameter
            if skip < 0:
                raise ValueError("skip parameter must be >= 0")

            # Validate limit parameter
            if limit < PaginationConfig.MIN_LIMIT:
                raise ValueError(
                    f"limit parameter must be >= {PaginationConfig.MIN_LIMIT}"
                )

            # Cap limit at maximum to prevent excessive queries
            if limit > PaginationConfig.MAX_LIMIT:
                self.logger.warning(
                    f"Limit {limit} exceeds maximum {PaginationConfig.MAX_LIMIT}, "
                    f"capping to maximum"
                )
                limit = PaginationConfig.MAX_LIMIT

            stmt = select(self.model).offset(skip).limit(limit)
            models = self.session.scalars(stmt).all()
            return [self.schema.model_validate(model) for model in models]

        except ValueError:
            raise
        except Exception as e:
            self.logger.error(f"Error finding all {self.model.__name__}: {e}")
            raise

    def save(self, model: BaseModel) -> BaseSchema:
        """
        Save a new record to the database

        Args:
            model: The model instance to save

        Returns:
            The saved schema instance
        """
        try:
            self.session.add(model)
            self.session.commit()
            self.session.refresh(model)
            return self.schema.model_validate(model)
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error saving {self.model.__name__}: {e}")
            raise

    def update(self, id_key: int, changes: dict) -> BaseSchema:
        """
        Update an existing record with security validation

        This method validates field names against the model's columns to prevent
        unauthorized updates to protected attributes or SQLAlchemy internals.

        Args:
            id_key: The primary key value
            changes: Dictionary of fields to update

        Returns:
            The updated schema instance

        Raises:
            InstanceNotFoundError: If the record is not found
            ValueError: If trying to update invalid or protected fields
        """
        # Protected attributes that should never be updated
        PROTECTED_ATTRIBUTES = {
            'id_key',  # Primary key
            '_sa_instance_state',  # SQLAlchemy internal
            '__class__',  # Python magic attribute
            '__dict__',  # Python magic attribute
        }

        try:
            stmt = select(self.model).where(self.model.id_key == id_key)
            instance = self.session.scalars(stmt).first()

            if instance is None:
                raise InstanceNotFoundError(
                    f"{self.model.__name__} with id {id_key} not found"
                )

            # Get allowed columns from model
            allowed_columns = {col.name for col in self.model.__table__.columns}

            # Validate and update only allowed fields
            for key, value in changes.items():
                # Skip None values
                if value is None:
                    continue

                # Check if key starts with underscore (internal attribute)
                if key.startswith('_'):
                    self.logger.warning(
                        f"Attempt to update protected attribute '{key}' blocked"
                    )
                    raise ValueError(
                        f"Cannot update protected attribute: {key}"
                    )

                # Check against protected list
                if key in PROTECTED_ATTRIBUTES:
                    self.logger.warning(
                        f"Attempt to update protected attribute '{key}' blocked"
                    )
                    raise ValueError(
                        f"Cannot update protected attribute: {key}"
                    )

                # Validate field exists in model
                if key not in allowed_columns:
                    self.logger.warning(
                        f"Attempt to update non-existent field '{key}' blocked"
                    )
                    raise ValueError(
                        f"Invalid field for {self.model.__name__}: {key}"
                    )

                # Validate attribute exists on instance
                if not hasattr(instance, key):
                    raise ValueError(
                        f"Field {key} not found in {self.model.__name__}"
                    )

                # All validations passed - safe to update
                setattr(instance, key, value)

            self.session.commit()
            self.session.refresh(instance)
            return self.schema.model_validate(instance)

        except InstanceNotFoundError:
            raise
        except ValueError:
            self.session.rollback()
            raise
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating {self.model.__name__} with id {id_key}: {e}")
            raise

    def remove(self, id_key: int) -> None:
        """
        Delete a record from the database

        Args:
            id_key: The primary key value

        Raises:
            InstanceNotFoundError: If the record is not found
        """
        try:
            stmt = select(self.model).where(self.model.id_key == id_key)
            model = self.session.scalars(stmt).first()

            if model is None:
                raise InstanceNotFoundError(
                    f"{self.model.__name__} with id {id_key} not found"
                )

            self.session.delete(model)
            self.session.commit()
        except InstanceNotFoundError:
            raise
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error deleting {self.model.__name__} with id {id_key}: {e}")
            raise

    def save_all(self, models: List[BaseModel]) -> List[BaseSchema]:
        """
        Save multiple records in a single transaction

        Args:
            models: List of model instances to save

        Returns:
            List of saved schema instances
        """
        try:
            self.session.add_all(models)
            self.session.commit()

            # Refresh all models
            for model in models:
                self.session.refresh(model)

            return [self.schema.model_validate(model) for model in models]
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error saving multiple {self.model.__name__}: {e}")
            raise