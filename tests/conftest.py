import pytest

from pgfast import DatabaseConfig, SchemaManager, close_pool, create_pool


@pytest.fixture
def db_config():
    """Test database configuration."""
    return DatabaseConfig(
        url="postgresql://@localhost:5432/pgfast_test",
        min_connections=2,
        max_connections=5,
    )


@pytest.fixture
async def manager(db_pool, tmp_path):
    """Create SchemaManager with temporary directories."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    return SchemaManager(
        pool=db_pool,
        migrations_dir=str(migrations_dir),
    )


@pytest.fixture
async def db_pool(db_config):
    """Provide a clean database pool."""
    pool = await create_pool(db_config)

    # Clean up any existing tables
    async with pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE")
        await conn.execute("CREATE SCHEMA public")

    yield pool

    await close_pool(pool)


