"""Initial database schema with all models

Revision ID: 001_initial_schema
Revises:
Create Date: 2025-11-17 18:05:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all tables with current schema including constraints and indexes"""

    # Create categories table
    op.create_table(
        'categories',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_categories_id_key'), 'categories', ['id_key'], unique=False)

    # Create clients table
    op.create_table(
        'clients',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('lastname', sa.String(), nullable=True),
        sa.Column('email', sa.String(), nullable=True),
        sa.Column('telephone', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id_key'),
        sa.UniqueConstraint('email')
    )
    op.create_index(op.f('ix_clients_email'), 'clients', ['email'], unique=True)
    op.create_index(op.f('ix_clients_id_key'), 'clients', ['id_key'], unique=False)

    # Create bills table
    op.create_table(
        'bills',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('bill_number', sa.String(), nullable=False),
        sa.Column('discount', sa.Float(), nullable=True),
        sa.Column('date', sa.Date(), nullable=True),
        sa.Column('total', sa.Float(), nullable=True),
        sa.Column('payment_type', sa.Integer(), nullable=True),  # Changed to Integer (was string)
        sa.PrimaryKeyConstraint('id_key'),
        sa.UniqueConstraint('bill_number')
    )
    op.create_index(op.f('ix_bills_bill_number'), 'bills', ['bill_number'], unique=True)
    op.create_index(op.f('ix_bills_id_key'), 'bills', ['id_key'], unique=False)

    # Create products table
    op.create_table(
        'products',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('price', sa.Float(), nullable=True),
        sa.Column('stock', sa.Integer(), nullable=True),
        sa.Column('category_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['category_id'], ['categories.id_key'], ),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_products_category_id'), 'products', ['category_id'], unique=False)
    op.create_index(op.f('ix_products_id_key'), 'products', ['id_key'], unique=False)

    # Create addresses table
    op.create_table(
        'addresses',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('street', sa.String(), nullable=True),
        sa.Column('city', sa.String(), nullable=True),
        sa.Column('state', sa.String(), nullable=True),
        sa.Column('zip_code', sa.String(), nullable=True),
        sa.Column('client_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id_key'], ),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_addresses_client_id'), 'addresses', ['client_id'], unique=False)
    op.create_index(op.f('ix_addresses_id_key'), 'addresses', ['id_key'], unique=False)

    # Create orders table
    op.create_table(
        'orders',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('date', sa.DateTime(), nullable=True),
        sa.Column('total', sa.Float(), nullable=True),
        sa.Column('delivery_method', sa.Integer(), nullable=True),  # Integer enum
        sa.Column('status', sa.Integer(), nullable=True),           # Integer enum
        sa.Column('client_id', sa.Integer(), nullable=True),
        sa.Column('bill_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['bill_id'], ['bills.id_key'], ),
        sa.ForeignKeyConstraint(['client_id'], ['clients.id_key'], ),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_orders_bill_id'), 'orders', ['bill_id'], unique=False)
    op.create_index(op.f('ix_orders_client_id'), 'orders', ['client_id'], unique=False)
    op.create_index(op.f('ix_orders_id_key'), 'orders', ['id_key'], unique=False)

    # Create reviews table with CHECK constraint for rating range
    op.create_table(
        'reviews',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('rating', sa.Float(), nullable=False),  # NOT NULL
        sa.Column('comment', sa.String(), nullable=True),
        sa.Column('product_id', sa.Integer(), nullable=True),
        sa.CheckConstraint('rating >= 1.0 AND rating <= 5.0', name='check_rating_range'),
        sa.ForeignKeyConstraint(['product_id'], ['products.id_key'], ),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_reviews_id_key'), 'reviews', ['id_key'], unique=False)
    op.create_index(op.f('ix_reviews_product_id'), 'reviews', ['product_id'], unique=False)

    # Create order_details table
    op.create_table(
        'order_details',
        sa.Column('id_key', sa.Integer(), nullable=False),
        sa.Column('quantity', sa.Integer(), nullable=True),
        sa.Column('price', sa.Float(), nullable=True),
        sa.Column('order_id', sa.Integer(), nullable=True),
        sa.Column('product_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['order_id'], ['orders.id_key'], ),
        sa.ForeignKeyConstraint(['product_id'], ['products.id_key'], ),
        sa.PrimaryKeyConstraint('id_key')
    )
    op.create_index(op.f('ix_order_details_id_key'), 'order_details', ['id_key'], unique=False)
    op.create_index(op.f('ix_order_details_order_id'), 'order_details', ['order_id'], unique=False)
    op.create_index(op.f('ix_order_details_product_id'), 'order_details', ['product_id'], unique=False)


def downgrade() -> None:
    """Drop all tables in reverse order"""
    op.drop_index(op.f('ix_order_details_product_id'), table_name='order_details')
    op.drop_index(op.f('ix_order_details_order_id'), table_name='order_details')
    op.drop_index(op.f('ix_order_details_id_key'), table_name='order_details')
    op.drop_table('order_details')

    op.drop_index(op.f('ix_reviews_product_id'), table_name='reviews')
    op.drop_index(op.f('ix_reviews_id_key'), table_name='reviews')
    op.drop_table('reviews')

    op.drop_index(op.f('ix_orders_id_key'), table_name='orders')
    op.drop_index(op.f('ix_orders_client_id'), table_name='orders')
    op.drop_index(op.f('ix_orders_bill_id'), table_name='orders')
    op.drop_table('orders')

    op.drop_index(op.f('ix_addresses_id_key'), table_name='addresses')
    op.drop_index(op.f('ix_addresses_client_id'), table_name='addresses')
    op.drop_table('addresses')

    op.drop_index(op.f('ix_products_id_key'), table_name='products')
    op.drop_index(op.f('ix_products_category_id'), table_name='products')
    op.drop_table('products')

    op.drop_index(op.f('ix_bills_id_key'), table_name='bills')
    op.drop_index(op.f('ix_bills_bill_number'), table_name='bills')
    op.drop_table('bills')

    op.drop_index(op.f('ix_clients_id_key'), table_name='clients')
    op.drop_index(op.f('ix_clients_email'), table_name='clients')
    op.drop_table('clients')

    op.drop_index(op.f('ix_categories_id_key'), table_name='categories')
    op.drop_table('categories')