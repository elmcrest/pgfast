"""pytest fixtures for pgfast testing.

Usage in conftest.py:
    from pgfast.pytest import *  # Import all fixtures

Or selectively:
    from pgfast.pytest import db_pool_factory, isolated_db
"""

import uuid
from pathlib import Path
from typing import AsyncGenerator

import asyncpg
import pytest

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool
from pgfast.schema import SchemaManager
from pgfast.testing import (
    TestDatabaseManager,
    cleanup_test_pool,
    create_test_pool_with_schema,
)


@pytest.fixture(scope="session")
def db_config():
    """Database configuration for tests.

    Override this in your conftest.py to customize.
    """
    import os

    url = os.getenv("TEST_DATABASE_URL", "postgresql://localhost/postgres")
    return DatabaseConfig(
        url=url,
        min_connections=2,
        max_connections=5,
    )


@pytest.fixture(scope="session")
def migrations_dir():
    """Path to migrations directory.

    Override in your conftest.py if using custom location.
    """
    return "db/migrations"


@pytest.fixture(scope="session")
def fixtures_dir():
    """Path to fixtures directory.

    Override in your conftest.py if using custom location.
    """
    return "db/fixtures"


@pytest.fixture(scope="session")
async def template_db(db_config, migrations_dir):
    """Create template database for faster test setup.

    Created once per test session, then cloned for each test.
    This provides significant speed improvements for large schemas.
    """
    # Check if migrations directory exists and has migrations
    migrations_path = Path(migrations_dir)
    if not migrations_path.exists() or not list(migrations_path.glob("*.sql")):
        # No migrations, skip template creation
        yield None
        return

    manager = TestDatabaseManager(db_config)
    template_name = f"pgfast_template_{uuid.uuid4().hex[:8]}"

    # Create template database
    pool = await manager.create_test_db(db_name=template_name)

    try:
        # Apply migrations to template
        schema_manager = SchemaManager(pool, migrations_dir)
        await schema_manager.schema_up()

        await close_pool(pool)

        # Mark as template
        admin_conn = await asyncpg.connect(
            db_config.url.rsplit("/", 1)[0] + "/postgres", timeout=db_config.timeout
        )
        try:
            await admin_conn.execute(
                """
                UPDATE pg_database
                SET datistemplate = TRUE
                WHERE datname = $1
                """,
                template_name,
            )
        finally:
            await admin_conn.close()

        yield template_name

    finally:
        # Cleanup: drop template database
        await manager.destroy_template_db(template_name)


@pytest.fixture
async def isolated_db(db_config, template_db) -> AsyncGenerator[asyncpg.Pool, None]:
    """Provide isolated test database with schema applied.

    Each test gets a fresh database cloned from template.
    Fast and fully isolated.
    """
    manager = TestDatabaseManager(db_config, template_db=template_db)
    pool = await manager.create_test_db()

    yield pool

    await manager.cleanup_test_db(pool)


@pytest.fixture
async def isolated_db_no_template(
    db_config, migrations_dir
) -> AsyncGenerator[asyncpg.Pool, None]:
    """Provide isolated test database without template.

    Use this if you don't want template optimization or need custom setup.
    """
    pool = await create_test_pool_with_schema(db_config, migrations_dir)

    yield pool

    await cleanup_test_pool(pool, db_config)


@pytest.fixture
async def db_pool_factory(db_config):
    """Factory fixture for creating multiple test databases.

    Useful when you need multiple databases in a single test.

    Example:
        async def test_multiple_databases(db_pool_factory):
            pool1 = await db_pool_factory()
            pool2 = await db_pool_factory()
            # Test cross-database operations
            await db_pool_factory.cleanup(pool1)
            await db_pool_factory.cleanup(pool2)
    """
    manager = TestDatabaseManager(db_config)
    created_pools = []

    async def _create() -> asyncpg.Pool:
        pool = await manager.create_test_db()
        created_pools.append(pool)
        return pool

    async def _cleanup(pool: asyncpg.Pool) -> None:
        await manager.cleanup_test_db(pool)
        if pool in created_pools:
            created_pools.remove(pool)

    _create.cleanup = _cleanup  # type: ignore

    yield _create

    # Cleanup any pools not explicitly cleaned up
    for pool in list(created_pools):
        try:
            await manager.cleanup_test_db(pool)
        except Exception:
            pass  # Best effort cleanup


@pytest.fixture
async def db_with_fixtures(isolated_db, fixtures_dir):
    """Database with fixtures loaded.

    Loads all SQL files from fixtures directory.
    """
    fixtures_path = Path(fixtures_dir)

    # Only load fixtures if directory exists
    if fixtures_path.exists():
        manager = TestDatabaseManager(
            DatabaseConfig(url="postgresql://localhost/postgres")
        )
        fixture_files = sorted(fixtures_path.glob("*.sql"))

        if fixture_files:
            await manager.load_fixtures(isolated_db, fixture_files)

    return isolated_db


__all__ = [
    "db_config",
    "migrations_dir",
    "fixtures_dir",
    "template_db",
    "isolated_db",
    "isolated_db_no_template",
    "db_pool_factory",
    "db_with_fixtures",
]
