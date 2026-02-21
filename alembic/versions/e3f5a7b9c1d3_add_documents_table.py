"""add documents table

Revision ID: e3f5a7b9c1d3
Revises: d2e4f6a8b0c2
Create Date: 2026-02-20 15:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e3f5a7b9c1d3"
down_revision: Union[str, None] = "d2e4f6a8b0c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("agency_id", sa.String(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("doc_hash", sa.String(64), nullable=False),
        sa.Column("document_type", sa.String(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("published_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "source_metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("cad_numbers", postgresql.JSONB(), nullable=True),
        sa.Column("foia_eligible", sa.Boolean(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["agency_id"], ["agencies.agency_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("doc_hash", name="uq_documents_doc_hash"),
    )
    op.create_index("ix_documents_agency_id", "documents", ["agency_id"])
    op.create_index("ix_documents_published_date", "documents", ["published_date"])


def downgrade() -> None:
    op.drop_index("ix_documents_published_date", table_name="documents")
    op.drop_index("ix_documents_agency_id", table_name="documents")
    op.drop_table("documents")
