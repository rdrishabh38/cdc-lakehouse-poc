"""initial_schema

Revision ID: fc68884dafd5
Revises: 
Create Date: 2026-09-25 01:08:55.820228

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fc68884dafd5'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the schema
    op.execute("CREATE SCHEMA IF NOT EXISTS inventory")
    
    # Create the table
    op.create_table(
        'customers',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('first_name', sa.String(length=255), nullable=False),
        sa.Column('last_name', sa.String(length=255), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False, unique=True),
        sa.Column('insert_timestamp', sa.DateTime(), nullable=True),
        sa.Column('update_timestamp', sa.DateTime(), nullable=True),
        schema='inventory'
    )
    
    # Set REPLICA IDENTITY FULL for Debezium to capture before-state of all columns
    op.execute("ALTER TABLE inventory.customers REPLICA IDENTITY FULL")


def downgrade() -> None:
    op.drop_table('customers', schema='inventory')
    op.execute("DROP SCHEMA IF EXISTS inventory")
