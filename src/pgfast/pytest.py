"""pytest fixtures for pgfast testing.

Usage in conftest.py:
    from pgfast.pytest import *  # Import all fixtures

Or selectively:
    from pgfast.pytest import db_pool_factory, isolated_db
"""

import uuid
from typing import Any, AsyncGenerator, Callable

import asyncpg
import pytest

from pgfast.config import DatabaseConfig
from pgfast.testing import (
    DatabaseTestManager,
    cleanup_test_pool,
    create_test_pool_with_schema,
)


@pytest.fixture(scope="session")
def db_config():
    """Database configuration for tests.

    Supports two configuration methods:
    1. TEST_DATABASE_URL environment variable (default: postgresql://localhost/postgres)
    2. TEST_POSTGRES_* fragment variables (TEST_POSTGRES_DB required)

    Override this in your conftest.py to customize.
    """
    import os

    # Try TEST_DATABASE_URL first
    url = os.getenv("TEST_DATABASE_URL")
    if url:
        return DatabaseConfig(
            url=url,
            min_connections=2,
            max_connections=5,
        )

    # Try TEST_POSTGRES_* fragments
    postgres_host = os.getenv("TEST_POSTGRES_HOST")
    postgres_port = os.getenv("TEST_POSTGRES_PORT")
    postgres_user = os.getenv("TEST_POSTGRES_USER")
    postgres_password = os.getenv("TEST_POSTGRES_PASSWORD")
    postgres_db = os.getenv("TEST_POSTGRES_DB")

    if postgres_db:
        # Build URL from TEST_POSTGRES_* fragments
        host = postgres_host or "localhost"
        port = postgres_port or "5432"
        user = postgres_user or "postgres"

        if postgres_password:
            auth = f"{user}:{postgres_password}"
        else:
            auth = user

        url = f"postgresql://{auth}@{host}:{port}/{postgres_db}"
        return DatabaseConfig(
            url=url,
            min_connections=2,
            max_connections=5,
        )

    # Default fallback
    return DatabaseConfig(
        url="postgresql://localhost/postgres",
        min_connections=2,
        max_connections=5,
    )


@pytest.fixture(scope="session")
async def template_db(db_config):
    """Create template database for faster test setup.

    Created once per test session, then cloned for each test.
    This provides significant speed improvements for large schemas.
    Uses auto-discovery to find all migrations directories.
    """
    # Check if any migrations exist (use **/*.sql to find migrations in subdirs)
    migrations_dirs = db_config.discover_migrations_dirs()
    has_migrations = False
    for migrations_dir in migrations_dirs:
        if migrations_dir.exists() and list(migrations_dir.glob("**/*.sql")):
            has_migrations = True
            break

    if not has_migrations:
        # No migrations, skip template creation
        yield None
        return

    manager = DatabaseTestManager(db_config)
    template_name = f"pgfast_template_{uuid.uuid4().hex[:8]}"

    try:
        # Create template database with migrations applied
        await manager.create_template_db(template_name)

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
    manager = DatabaseTestManager(db_config, template_db=template_db)
    pool = await manager.create_test_db()

    yield pool

    await manager.cleanup_test_db(pool)


@pytest.fixture
async def isolated_db_no_template(db_config) -> AsyncGenerator[asyncpg.Pool, None]:
    """Provide isolated test database without template.

    Use this if you don't want template optimization or need custom setup.
    """
    pool = await create_test_pool_with_schema(db_config)

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
    manager = DatabaseTestManager(db_config)
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
async def db_with_fixtures(isolated_db, db_config):
    """Database with fixtures loaded.

    Auto-discovers and loads all SQL files from all configured fixture directories.
    """
    manager = DatabaseTestManager(db_config)

    # Auto-discover and load fixtures
    await manager.load_fixtures(isolated_db, fixtures=None)

    return isolated_db


@pytest.fixture
async def fixture_loader(isolated_db, db_config):
    """Fixture to load specific fixtures by name.

    Usage:
        async def test_something(isolated_db, fixture_loader):
            await fixture_loader(["users", "products"])
            # ...
    """
    manager = DatabaseTestManager(db_config)

    async def load(names: list[str]) -> None:
        from pgfast.fixtures import Fixture

        # Discover all fixtures (returns paths sorted by dependency order)
        all_fixture_paths = manager.discover_fixtures()

        # Parse fixtures once and filter by requested names
        # Maintains dependency order since all_fixture_paths is already sorted
        to_load = []
        found_names = set()

        for path in all_fixture_paths:
            fixture = Fixture.from_path(path)
            if fixture and fixture.name in names:
                to_load.append(path)
                found_names.add(fixture.name)

        # Check for missing fixtures
        missing = set(names) - found_names
        if missing:
            raise ValueError(f"Fixtures not found: {', '.join(missing)}")

        await manager.load_fixtures(isolated_db, to_load)

    return load


@pytest.fixture
def test_client(isolated_db):
    """Factory for creating FastAPI test clients with isolated database.

    This fixture simplifies testing FastAPI endpoints by automatically
    overriding the database pool dependency.

    Usage in conftest.py:
        from fastapi import FastAPI
        from pgfast.pytest import test_client as base_test_client
        from app import app, get_db_pool

        @pytest.fixture
        async def api_client(base_test_client):
            async with base_test_client(app, get_db_pool) as client:
                yield client

    Then in tests:
        async def test_endpoint(api_client):
            response = await api_client.get("/todos")
            assert response.status_code == 200

    Args:
        app: FastAPI application instance
        pool_dependency: The dependency function to override (e.g., get_db_pool)
        base_url: Base URL for the test client (default: "http://test")

    Returns:
        AsyncClient with pool dependency overridden to use isolated_db
    """
    try:
        from httpx import ASGITransport, AsyncClient
    except ImportError:
        raise ImportError(
            "httpx is required for test_client fixture. "
            "Install it with: pip install httpx"
        )

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _create_client(
        app: Any,
        pool_dependency: Callable[[], asyncpg.Pool],
        base_url: str = "http://test",
    ) -> AsyncGenerator[AsyncClient, None]:
        """Create test client with dependency override."""
        # Store original overrides
        original_overrides = app.dependency_overrides.copy()

        try:
            # Override pool dependency to use isolated test database
            app.dependency_overrides[pool_dependency] = lambda: isolated_db

            # Create ASGI transport and test client
            transport = ASGITransport(app=app)  # type: ignore
            async with AsyncClient(transport=transport, base_url=base_url) as client:
                yield client

        finally:
            # Restore original overrides
            app.dependency_overrides = original_overrides

    return _create_client


__all__ = [
    "db_config",
    "template_db",
    "isolated_db",
    "isolated_db_no_template",
    "db_pool_factory",
    "db_with_fixtures",
    "fixture_loader",
    "test_client",
]
