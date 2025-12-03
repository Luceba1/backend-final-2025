-- Migration: Add stock index and check constraint
-- Date: 2025-11-17
-- Author: Critical Fixes Implementation
-- Description: Adds index on products.stock and check constraint for stock >= 0

-- 1. Add index on products.stock for better query performance
CREATE INDEX IF NOT EXISTS ix_products_stock ON products(stock);

-- 2. Add check constraint to ensure stock is never negative
-- Note: If this fails with "constraint already exists", it's safe to ignore
ALTER TABLE products
ADD CONSTRAINT check_product_stock_non_negative
CHECK (stock >= 0);

-- Verify changes
SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'products'
  AND indexname = 'ix_products_stock';

SELECT
    conname,
    pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'products'::regclass
  AND contype = 'c'
  AND conname = 'check_product_stock_non_negative';