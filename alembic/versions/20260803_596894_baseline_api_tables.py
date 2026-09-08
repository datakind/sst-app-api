"""Baseline API-owned tables.

Revision ID: 20260803_596894
Revises:
Create Date: 2026-08-03

Creates API-owned tables as they stood at the Alembic cutover. Does not create
`users` (edvise-ui / Laravel DDL ownership).

On existing Cloud SQL environments: run `alembic stamp head` instead of
`upgrade` for the first cutover (tables already exist via create_all).

The DDL below is frozen at cutover and intentionally does not read
`Base.metadata`. Later ORM changes ship as their own ALTER revisions, so this
revision must keep describing the original schema.

Prefer generating future revisions with `alembic revision -m "..."` so
Alembic assigns a unique revision id (random hex by default).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260803_596894"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "inst",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("retention_days", sa.Integer(), nullable=True),
        sa.Column("allowed_emails", sa.JSON(), nullable=False),
        sa.Column("schemas", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(length=36), nullable=True),
        sa.Column("pdp_id", sa.String(length=36), nullable=True),
        sa.Column("edvise_id", sa.String(length=36), nullable=True),
        sa.Column("legacy_id", sa.String(length=36), nullable=True),
        sa.Column("genai_id", sa.String(length=36), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("genai_id"),
        sa.UniqueConstraint("name"),
        sa.UniqueConstraint("name", "state", name="inst_name_state_uc"),
    )
    op.create_table(
        "apikey",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("hashed_key_value", sa.String(length=255), nullable=False),
        sa.Column("inst_id", sa.Uuid(), nullable=True),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("notes", sa.String(length=255), nullable=True),
        sa.Column("allows_enduser", sa.Boolean(), nullable=True),
        sa.Column("access_type", sa.String(length=36), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=True),
        sa.Column("valid", sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(["inst_id"], ["inst.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("hashed_key_value"),
        sa.UniqueConstraint("inst_id", "access_type", name="apikeys_inst_access_uc"),
    )
    op.create_table(
        "batch",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("inst_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=True),
        sa.Column("completed", sa.Boolean(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(["inst_id"], ["inst.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "inst_id", name="batch_name_inst_uc"),
    )
    op.create_table(
        "file",
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("inst_id", sa.Uuid(), nullable=False),
        sa.Column("uploader", sa.Uuid(), nullable=True),
        sa.Column("source", sa.String(length=36), nullable=True),
        sa.Column("schemas", sa.JSON(), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retention_days", sa.Integer(), nullable=True),
        sa.Column("sst_generated", sa.Boolean(), nullable=False),
        sa.Column("valid", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["inst_id"], ["inst.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "inst_id", name="file_name_inst_uc"),
    )
    op.create_table(
        "model",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("inst_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("schema_configs", sa.JSON(), nullable=True),
        sa.Column("created_by", sa.Uuid(), nullable=True),
        sa.Column("deleted", sa.Boolean(), nullable=True),
        sa.Column("valid", sa.Boolean(), nullable=True),
        sa.Column("archived", sa.Integer(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["inst_id"], ["inst.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "inst_id", name="model_name_inst_uc"),
    )
    op.create_table(
        "schema_registry",
        sa.Column("schema_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "doc_type",
            sa.Enum("base", "extension", name="doctype", native_enum=False),
            nullable=False,
        ),
        sa.Column("inst_id", sa.Uuid(), nullable=True),
        sa.Column("is_pdp", sa.Boolean(), nullable=False),
        sa.Column("is_edvise", sa.Boolean(), nullable=False),
        sa.Column("version_label", sa.String(length=255), nullable=False),
        sa.Column("extends_schema_id", sa.Integer(), nullable=True),
        sa.Column("json_doc", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "NOT (is_pdp = 1 AND is_edvise = 1)", name="ck_no_pdp_and_edvise"
        ),
        sa.ForeignKeyConstraint(
            ["extends_schema_id"],
            ["schema_registry.schema_id"],
            onupdate="CASCADE",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["inst_id"], ["inst.id"], onupdate="CASCADE", ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("schema_id"),
        sa.UniqueConstraint("doc_type", "version_label", name="uq_base_version"),
        sa.UniqueConstraint("inst_id", "version_label", name="uq_inst_version"),
        sa.UniqueConstraint("is_pdp", "version_label", name="uq_pdp_version"),
    )
    op.create_index(
        "idx_schema_active_base",
        "schema_registry",
        ["doc_type", "is_active"],
        unique=False,
    )
    op.create_index(
        "idx_schema_active_edvise",
        "schema_registry",
        ["is_edvise", "is_active"],
        unique=False,
    )
    op.create_index(
        "idx_schema_active_inst",
        "schema_registry",
        ["inst_id", "is_active"],
        unique=False,
    )
    op.create_index(
        "idx_schema_active_pdp",
        "schema_registry",
        ["is_pdp", "is_active"],
        unique=False,
    )
    op.create_table(
        "account_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("inst_id", sa.Uuid(), nullable=True),
        sa.Column("action", sa.String(length=255), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["inst_id"], ["inst.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "file_batch_association_table",
        sa.Column("file_val", sa.Uuid(), nullable=False),
        sa.Column("batch_val", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["batch_val"], ["batch.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["file_val"], ["file.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("file_val", "batch_val"),
    )
    op.create_table(
        "job",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("model_id", sa.Uuid(), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("batch_name", sa.String(length=255), nullable=False),
        sa.Column("output_filename", sa.String(length=255), nullable=True),
        sa.Column("output_valid", sa.Boolean(), nullable=True),
        sa.Column("err_msg", sa.String(length=255), nullable=True),
        sa.Column("completed", sa.Boolean(), nullable=True),
        sa.Column("model_version", sa.String(length=255), nullable=True),
        sa.Column("model_run_id", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["model_id"], ["model.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    raise NotImplementedError(
        "Baseline downgrade is refused — dropping API tables on shared Cloud SQL "
        "is unsafe. Restore from backup instead."
    )
