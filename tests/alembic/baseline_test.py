"""Regression checks for the Alembic baseline (API-owned tables only)."""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from webapp.database import Base

# Keep in sync with alembic/versions/20260803_596894_baseline_api_tables.py
API_TABLES = {
    "inst",
    "apikey",
    "account_history",
    "file",
    "batch",
    "file_batch_association_table",
    "model",
    "schema_registry",
    "job",
}

BASELINE_REVISION = "20260803_596894"
REPO_ROOT = Path(__file__).resolve().parents[2]


def _script_head(cfg: Config) -> str:
    """Current head revision, so adding a revision does not break these tests."""
    head = ScriptDirectory.from_config(cfg).get_current_head()
    assert head is not None
    return head


@pytest.fixture()
def alembic_cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Config, Path]:
    db_path = tmp_path / "alembic_test.db"
    monkeypatch.setenv("ENV", "LOCAL")
    monkeypatch.setenv("ALEMBIC_SQLITE_URL", f"sqlite:///{db_path}")
    monkeypatch.delenv("ALEMBIC_DATABASE_URL", raising=False)

    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg, db_path


def test_api_tables_match_orm_metadata_minus_users() -> None:
    """Fail CI if a new mapped table is omitted from the baseline list."""
    orm_api_tables = {
        name
        for name in Base.metadata.tables
        if name != "users" and not name.endswith("_backup")
    }
    assert API_TABLES == orm_api_tables


def test_alembic_upgrade_creates_api_tables_without_users(
    alembic_cfg: tuple[Config, Path],
) -> None:
    cfg, db_path = alembic_cfg
    command.upgrade(cfg, "head")

    engine = create_engine(f"sqlite:///{db_path}")
    tables = set(inspect(engine).get_table_names())
    assert API_TABLES.issubset(tables)
    assert "users" not in tables
    assert "alembic_version" in tables
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
    assert version == _script_head(cfg)


def test_alembic_upgrade_is_noop_after_stamp(
    alembic_cfg: tuple[Config, Path],
) -> None:
    cfg, db_path = alembic_cfg

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(
        engine, tables=[Base.metadata.tables[name] for name in API_TABLES]
    )

    command.stamp(cfg, "head")
    with engine.connect() as conn:
        stamped = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
    assert stamped == _script_head(cfg)

    before = set(inspect(engine).get_table_names())
    command.upgrade(cfg, "head")
    after = set(inspect(engine).get_table_names())
    assert before == after
    assert "users" not in after

    with engine.connect() as conn:
        after_version = conn.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar()
    assert after_version == _script_head(cfg)
