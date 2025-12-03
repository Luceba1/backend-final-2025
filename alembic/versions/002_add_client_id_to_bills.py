"""Add client_id to bills table

Revision ID: 002_add_client_id
Revises: 001_initial_schema
Create Date: 2025-11-17 18:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '002_add_client_id'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add client_id column to bills table with foreign key and index"""

    # Step 1: Add client_id column (nullable initially to allow backfill)
    op.add_column('bills', sa.Column('client_id', sa.Integer(), nullable=True))

    # Step 2: Backfill client_id from orders table
    # Every bill is associated with an order, and every order has a client_id
    op.execute("""
        UPDATE bills
        SET client_id = (
            SELECT orders.client_id
            FROM orders
            WHERE orders.bill_id = bills.id_key
            LIMIT 1
        )
    """)

    # Step 3: Make client_id NOT NULL after backfilling
    op.alter_column('bills', 'client_id', nullable=False)

    # Step 4: Create foreign key constraint
    op.create_foreign_key(
        'fk_bills_client_id',  # Constraint name
        'bills',                # Source table
        'clients',              # Referenced table
        ['client_id'],          # Source columns
        ['id_key']              # Referenced columns
    )

    # Step 5: Create index for performance
    op.create_index(op.f('ix_bills_client_id'), 'bills', ['client_id'], unique=False)


def downgrade() -> None:
    """Remove client_id column from bills table"""

    # Remove index
    op.drop_index(op.f('ix_bills_client_id'), table_name='bills')

    # Remove foreign key
    op.drop_constraint('fk_bills_client_id', 'bills', type_='foreignkey')

    # Remove column
    op.drop_column('bills', 'client_id')