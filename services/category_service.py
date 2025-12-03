"""Category service with Redis caching integration."""
import logging
from typing import List
from sqlalchemy.orm import Session

from models.category import CategoryModel
from repositories.category_repository import CategoryRepository
from schemas.category_schema import CategorySchema
from services.base_service_impl import BaseServiceImpl
from services.cache_service import cache_service
from utils.logging_utils import get_sanitized_logger

logger = get_sanitized_logger(__name__)


class CategoryService(BaseServiceImpl):
    """Service for Category entity with aggressive caching (rarely changes)."""

    def __init__(self, db: Session):
        super().__init__(
            repository_class=CategoryRepository,
            model=CategoryModel,
            schema=CategorySchema,
            db=db
        )
        self.cache = cache_service
        self.cache_prefix = "categories"
        self.cache_ttl = 3600  # 1 hour

    def get_all(self, skip: int = 0, limit: int = 100) -> List[CategorySchema]:
        """
        Get all categories with long-lived cache
        Key pattern: categories:list:skip:{skip}:limit:{limit}
        """

        # FIX — Convertir en partes posicionales (build_key no acepta kwargs)
        cache_key = self.cache.build_key(
            self.cache_prefix,
            "list",
            f"skip:{skip}",
            f"limit:{limit}"
        )

        cached_categories = self.cache.get(cache_key)
        if cached_categories is not None:
            logger.debug(f"Cache HIT: {cache_key}")
            return [CategorySchema(**c) for c in cached_categories]

        logger.debug(f"Cache MISS: {cache_key}")
        categories = super().get_all(skip, limit)

        categories_dict = [c.model_dump() for c in categories]
        self.cache.set(cache_key, categories_dict, ttl=self.cache_ttl)

        return categories

    def get_one(self, id_key: int) -> CategorySchema:
        """
        Cache key pattern: categories:id:{id_key}
        """

        # FIX — no kwargs
        cache_key = self.cache.build_key(
            self.cache_prefix,
            "id",
            f"id:{id_key}"
        )

        cached_category = self.cache.get(cache_key)
        if cached_category is not None:
            logger.debug(f"Cache HIT: {cache_key}")
            return CategorySchema(**cached_category)

        logger.debug(f"Cache MISS: {cache_key}")
        category = super().get_one(id_key)

        self.cache.set(cache_key, category.model_dump(), ttl=self.cache_ttl)

        return category

    def save(self, schema: CategorySchema) -> CategorySchema:
        category = super().save(schema)
        self._invalidate_all_cache()
        return category

    def update(self, id_key: int, schema: CategorySchema) -> CategorySchema:
        try:
            category = super().update(id_key, schema)
            self._invalidate_all_cache()
            logger.info(f"Category {id_key} updated and cache invalidated successfully")
            return category
        except Exception as e:
            logger.error(f"Failed to update category {id_key}: {e}")
            raise

    def delete(self, id_key: int) -> None:
        super().delete(id_key)
        self._invalidate_all_cache()

    def _invalidate_all_cache(self):
        pattern = f"{self.cache_prefix}:*"
        deleted_count = self.cache.delete_pattern(pattern)
        if deleted_count > 0:
            logger.info(f"Invalidated {deleted_count} category cache entries")
