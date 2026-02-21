"""add crimemapping_agency_id to agencies

Revision ID: b8f2e1a9c43d
Revises: 01dcc9e5d8c1
Create Date: 2026-02-20 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "b8f2e1a9c43d"
down_revision: Union[str, None] = "01dcc9e5d8c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "agencies",
        sa.Column("crimemapping_agency_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agencies", "crimemapping_agency_id")
