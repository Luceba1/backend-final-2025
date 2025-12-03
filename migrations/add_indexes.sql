-- =============================================================================
-- DATABASE INDEX MIGRATION SCRIPT
-- =============================================================================
-- Purpose: Add indexes to foreign key columns for query performance
-- Date: 2025-11-17
-- Status: Already implemented in SQLAlchemy models
--
-- NOTE: This script is for reference and manual database setup only.
-- The application automatically creates these indexes via SQLAlchemy models.
-- =============================================================================

-- Check existing indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- =============================================================================
-- ORDERS TABLE INDEXES
-- =============================================================================
-- These indexes already exist via SQLAlchemy (index=True in models)

-- Index on client_id for queries like "get all orders for client X"
-- CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);

-- Index on bill_id for queries like "get order for bill X"
-- CREATE INDEX IF NOT EXISTS idx_orders_bill_id ON orders(bill_id);

-- Index on date for time-based queries
-- CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);

-- Index on status for filtering by order status
-- CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Index on delivery_method for filtering
-- CREATE INDEX IF NOT EXISTS idx_orders_delivery_method ON orders(delivery_method);

-- =============================================================================
-- ORDER_DETAILS TABLE INDEXES
-- =============================================================================

-- Index on order_id for queries like "get all details for order X"
-- CREATE INDEX IF NOT EXISTS idx_order_details_order_id ON order_details(order_id);

-- Index on product_id for queries like "get all orders containing product X"
-- CREATE INDEX IF NOT EXISTS idx_order_details_product_id ON order_details(product_id);

-- Composite index for common join queries
-- CREATE INDEX IF NOT EXISTS idx_order_details_order_product ON order_details(order_id, product_id);

-- =============================================================================
-- REVIEWS TABLE INDEXES
-- =============================================================================

-- Index on product_id for queries like "get all reviews for product X"
-- CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);

-- Index on rating for filtering by rating
-- CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);

-- =============================================================================
-- ADDRESSES TABLE INDEXES
-- =============================================================================

-- Index on client_id for queries like "get all addresses for client X"
-- CREATE INDEX IF NOT EXISTS idx_addresses_client_id ON addresses(client_id);

-- Index on street for address lookup
-- CREATE INDEX IF NOT EXISTS idx_addresses_street ON addresses(street);

-- =============================================================================
-- PRODUCTS TABLE INDEXES
-- =============================================================================

-- Index on category_id for queries like "get all products in category X"
-- CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);

-- Index on name for product search
-- CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);

-- Index on price for price-based queries
-- CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);

-- =============================================================================
-- BILLS TABLE INDEXES
-- =============================================================================

-- Unique index on bill_number (already created via unique=True)
-- CREATE UNIQUE INDEX IF NOT EXISTS idx_bills_bill_number ON bills(bill_number);

-- =============================================================================
-- PERFORMANCE VERIFICATION QUERIES
-- =============================================================================

-- Check index usage statistics
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Find missing indexes (slow queries without indexes)
SELECT
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    seq_tup_read / seq_scan as avg_seq_tup_read
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND seq_scan > 0
ORDER BY seq_tup_read DESC;

-- =============================================================================
-- MAINTENANCE COMMANDS
-- =============================================================================

-- Reindex all tables (run during maintenance window)
-- REINDEX TABLE orders;
-- REINDEX TABLE order_details;
-- REINDEX TABLE reviews;
-- REINDEX TABLE addresses;
-- REINDEX TABLE products;
-- REINDEX TABLE bills;

-- Analyze tables to update statistics
ANALYZE orders;
ANALYZE order_details;
ANALYZE reviews;
ANALYZE addresses;
ANALYZE products;
ANALYZE bills;
ANALYZE clients;
ANALYZE categories;

-- =============================================================================
-- NOTES
-- =============================================================================
-- 1. All indexes are already defined in SQLAlchemy models with index=True
-- 2. Indexes are automatically created when running create_tables()
-- 3. This script is for manual database setup or migration from existing DB
-- 4. Always run ANALYZE after creating indexes to update query planner statistics
-- 5. Monitor index usage with pg_stat_user_indexes to identify unused indexes
-- =============================================================================