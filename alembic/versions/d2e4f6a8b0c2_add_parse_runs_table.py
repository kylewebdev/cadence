"""add parse_runs table

Revision ID: d2e4f6a8b0c2
Revises: c1f3a2b4d5e6
Create Date: 2026-02-20 14:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "d2e4f6a8b0c2"
down_revision: Union[str, None] = "c1f3a2b4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "parse_runs",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("agency_id", sa.String(), nullable=False),
        sa.Column(
            "run_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("docs_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("feeds_scraped", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("platform_type", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["agency_id"], ["agencies.agency_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_parse_runs_agency_id_run_at",
        "parse_runs",
        ["agency_id", "run_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_parse_runs_agency_id_run_at", table_name="parse_runs")
    op.drop_table("parse_runs")
