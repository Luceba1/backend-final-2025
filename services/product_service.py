"""Product service with Redis caching integration and sanitized logging."""
import logging
from typing import List, Optional
from sqlalchemy.orm import Session

from models.product import ProductModel
from repositories.product_repository import ProductRepository
from schemas.product_schema import ProductSchema
from services.base_service_impl import BaseServiceImpl
from services.cache_service import cache_service
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)  # P11: Sanitized logging


class ProductService(BaseServiceImpl):
    """Service for Product entity with caching."""

    def __init__(self, db: Session):
        super().__init__(
            repository_class=ProductRepository,
            model=ProductModel,
            schema=ProductSchema,
            db=db
        )
        self.cache = cache_service
        self.cache_prefix = "products"

    def get_all(self, skip: int = 0, limit: int = 100) -> List[ProductSchema]:
        """
        Get all products with caching
        """
        cache_key = self.cache.build_key(
            self.cache_prefix,
            "list",
            skip=skip,
            limit=limit
        )

        # Try to read cache
        cached_products = self.cache.get(cache_key)
        if cached_products is not None:
            if isinstance(cached_products, list):
                logger.debug(f"Cache HIT: {cache_key}")
                return [ProductSchema(**p) for p in cached_products]
            else:
                logger.warning(f"⚠️ Invalid cached data at {cache_key}, ignoring cache")

        # Cache miss
        logger.debug(f"Cache MISS: {cache_key}")
        products = super().get_all(skip, limit)

        # Store in cache
        try:
            products_dict = [p.model_dump() for p in products]
            self.cache.set(cache_key, products_dict)
        except Exception as e:
            logger.error(f"Failed to set cache for {cache_key}: {e}")

        return products

    def get_one(self, id_key: int) -> ProductSchema:
        cache_key = self.cache.build_key(self.cache_prefix, "id", id=id_key)

        cached_product = self.cache.get(cache_key)
        if cached_product is not None:
            if isinstance(cached_product, dict):
                logger.debug(f"Cache HIT: {cache_key}")
                return ProductSchema(**cached_product)
            else:
                logger.warning(f"⚠️ Invalid cached data at {cache_key}, ignoring cache")

        logger.debug(f"Cache MISS: {cache_key}")
        product = super().get_one(id_key)

        try:
            self.cache.set(cache_key, product.model_dump())
        except Exception as e:
            logger.error(f"Failed to set cache for {cache_key}: {e}")

        return product

    def save(self, schema: ProductSchema) -> ProductSchema:
        product = super().save(schema)
        self._invalidate_list_cache()
        return product

    def update(self, id_key: int, schema: ProductSchema) -> ProductSchema:
        cache_key = self.cache.build_key(self.cache_prefix, "id", id=id_key)

        try:
            product = super().update(id_key, schema)
            self.cache.delete(cache_key)
            self._invalidate_list_cache()
            logger.info(f"Product {id_key} updated and cache invalidated successfully")
            return product

        except Exception as e:
            logger.error(f"Failed to update product {id_key}: {e}")
            raise

    def delete(self, id_key: int) -> None:
        from models.order_detail import OrderDetailModel
        from sqlalchemy import select

        stmt = select(OrderDetailModel).where(
            OrderDetailModel.product_id == id_key
        ).limit(1)

        has_sales = self._repository.session.scalars(stmt).first()

        if has_sales:
            logger.error(
                f"Cannot delete product {id_key}: has associated sales history"
            )
            raise ValueError(
                f"Cannot delete product {id_key}: product has associated sales history."
            )

        logger.info(f"Deleting product {id_key}")
        super().delete(id_key)

        cache_key = self.cache.build_key(self.cache_prefix, "id", id=id_key)
        self.cache.delete(cache_key)
        self._invalidate_list_cache()

    def _invalidate_list_cache(self):
        pattern = f"{self.cache_prefix}:list:*"
        deleted_count = self.cache.delete_pattern(pattern)
        if deleted_count > 0:
            logger.info(f"Invalidated {deleted_count} product list cache entries")
