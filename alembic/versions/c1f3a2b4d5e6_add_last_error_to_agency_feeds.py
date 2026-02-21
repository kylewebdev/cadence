"""add last_error to agency_feeds

Revision ID: c1f3a2b4d5e6
Revises: b8f2e1a9c43d
Create Date: 2026-02-20 13:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c1f3a2b4d5e6"
down_revision: Union[str, None] = "b8f2e1a9c43d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "agency_feeds",
        sa.Column("last_error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agency_feeds", "last_error")
