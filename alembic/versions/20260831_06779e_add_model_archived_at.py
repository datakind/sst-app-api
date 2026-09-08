"""add_model_archived_at

Revision ID: 20260831_06779e
Revises: 20260803_596894
Create Date: 2026-08-31 09:52:40.107705

Adds `model.archived_at` so archived models can report when they were archived.
Nullable: existing rows (including already-archived ones) stay NULL rather than
claiming a migration-time archive date.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260831_06779e"
down_revision: Union[str, Sequence[str], None] = "20260803_596894"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "model", sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("model", "archived_at")
