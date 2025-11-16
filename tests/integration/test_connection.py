import pytest
import asyncpg
from pgfast.config import DatabaseConfig
from pgfast.connection import create_pool, close_pool
from pgfast.exceptions import ConnectionError


@pytest.fixture
def db_config():
    """Test database configuration."""
    return DatabaseConfig(
        url="postgresql://@localhost:5432/pgfast_test",
        min_connections=2,
        max_connections=5,
    )


@pytest.mark.asyncio
async def test_create_pool_success(db_config):
    """Should create a connection pool successfully."""
    pool = await create_pool(db_config)

    assert pool is not None
    assert isinstance(pool, asyncpg.Pool)

    # Verify we can execute a query
    async with pool.acquire() as conn:
        result = await conn.fetchval("SELECT 1")
        assert result == 1

    await close_pool(pool)


@pytest.mark.asyncio
async def test_pool_respects_configuration(db_config):
    """Pool should respect min/max connection settings."""
    pool = await create_pool(db_config)

    # Pool should report correct settings
    assert pool.get_min_size() == db_config.min_connections
    assert pool.get_max_size() == db_config.max_connections

    await close_pool(pool)


@pytest.mark.asyncio
async def test_create_pool_with_invalid_url():
    """Should raise ConnectionError for invalid database URL."""
    bad_config = DatabaseConfig(
        url="postgresql://baduser:badpass@localhost:5432/nonexistent"
    )

    with pytest.raises(ConnectionError, match="Failed to connect"):
        await create_pool(bad_config)


@pytest.mark.asyncio
async def test_close_pool_gracefully(db_config):
    """Should close pool and release all connections."""
    pool = await create_pool(db_config)

    # Acquire a connection
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")

    # Close pool
    await close_pool(pool)

    # Pool should be closed
    assert pool._closed  # Access internal state for testing


@pytest.mark.asyncio
async def test_pool_connection_health_check(db_config):
    """Should verify connections are healthy."""
    pool = await create_pool(db_config)

    # Execute multiple queries to verify connection health
    for i in range(5):
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT $1::int", i)
            assert result == i

    await close_pool(pool)
