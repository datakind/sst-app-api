"""Alembic environment for edvise-api.

Manages API-owned tables only. Excludes `users` (Laravel / edvise-ui DDL)
and any `*_backup` tables that may exist on shared Cloud SQL instances.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import pool, create_engine
from sqlalchemy.engine.url import URL

from webapp.alembic_filters import include_object
from webapp.config import db_connection, ssl_connect_args
from webapp.database import Base

_env_file = os.environ.get("ENV_FILE_PATH")
if _env_file:
    load_dotenv(_env_file)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(
            f"Missing {name} value. Required for Alembic database connection."
        )
    return value


def _database_url() -> str:
    """Build SQLAlchemy URL from the same env vars as the webapp (or override)."""
    override = os.environ.get("ALEMBIC_DATABASE_URL")
    if override:
        return override

    if db_connection() == "sqlite":
        return os.environ.get("ALEMBIC_SQLITE_URL", "sqlite:///./alembic_local.db")

    return URL.create(
        drivername="mysql+pymysql",
        username=_require_env("DB_USER"),
        password=os.environ.get("DB_PASS") or "",
        host=_require_env("INSTANCE_HOST"),
        port=int(os.environ.get("DB_PORT", "3306")),
        database=_require_env("DB_NAME"),
    ).render_as_string(hide_password=False)


def run_migrations_offline() -> None:
    url = _database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = _database_url()

    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
        connect_args=ssl_connect_args() if db_connection() == "mysql" else {},
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
