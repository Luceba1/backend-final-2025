"""
Module for the abstract base controller class.

This base class has been simplified to work with the dependency injection pattern.
Controllers no longer need to implement abstract methods directly - they register
FastAPI route handlers through _register_routes() instead.
"""

from abc import ABC


class BaseController(ABC):
    """
    Abstract base controller class for FastAPI controllers.

    This class provides a minimal interface for controllers.
    Concrete implementations handle route registration via dependency injection.
    """
    pass
