"""Integration tests for schema management."""

import pytest
import pytest_asyncio
from pgfast.config import DatabaseConfig
from pgfast.connection import create_pool, close_pool
from pgfast.schema import SchemaManager
from pgfast.exceptions import SchemaError


@pytest.fixture
def db_config():
    """Test database configuration."""
    return DatabaseConfig(url="postgresql://localhost:5432/pgfast_test")


@pytest_asyncio.fixture
async def db_pool(db_config):
    """Provide a clean database pool."""
    pool = await create_pool(db_config)

    # Clean up any existing tables
    async with pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE")
        await conn.execute("CREATE SCHEMA public")

    yield pool

    await close_pool(pool)


@pytest.fixture
def schema_dir(tmp_path):
    """Create temporary schema directory with test SQL files."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    # Create a simple schema file
    schema_file = schema_dir / "20240101_001_initial.sql"
    schema_file.write_text("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX idx_users_email ON users(email);
    """)

    return schema_dir


@pytest.mark.asyncio
async def test_apply_schema_creates_tables(db_pool, schema_dir):
    """Should apply schema and create tables."""
    manager = SchemaManager(db_pool, schema_dir=str(schema_dir))

    await manager.apply_schema()

    # Verify table exists
    async with db_pool.acquire() as conn:
        result = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'users'
            )
        """)
        assert result is True


@pytest.mark.asyncio
async def test_apply_schema_executes_all_statements(db_pool, schema_dir):
    """Should execute all statements in schema file."""
    manager = SchemaManager(db_pool, schema_dir=str(schema_dir))

    await manager.apply_schema()

    # Verify index exists
    async with db_pool.acquire() as conn:
        result = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM pg_indexes
                WHERE schemaname = 'public'
                AND tablename = 'users'
                AND indexname = 'idx_users_email'
            )
        """)
        assert result is True


@pytest.mark.asyncio
async def test_apply_schema_is_transactional(db_pool, tmp_path):
    """Schema application should rollback on error."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    # Create schema with error in middle
    schema_file = schema_dir / "20240101_001_bad.sql"
    schema_file.write_text("""
        CREATE TABLE test1 (id SERIAL PRIMARY KEY);
        THIS IS INVALID SQL;
        CREATE TABLE test2 (id SERIAL PRIMARY KEY);
    """)

    manager = SchemaManager(db_pool, schema_dir=str(schema_dir))

    with pytest.raises(SchemaError):
        await manager.apply_schema()

    # Verify no tables were created (full rollback)
    async with db_pool.acquire() as conn:
        result = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('test1', 'test2')
        """)
        assert result == 0


@pytest.mark.asyncio
async def test_apply_schema_with_multiple_files(db_pool, tmp_path):
    """Should apply multiple schema files in order."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    # Create multiple schema files
    (schema_dir / "20240101_001_users.sql").write_text(
        "CREATE TABLE users (id SERIAL PRIMARY KEY);"
    )
    (schema_dir / "20240101_002_posts.sql").write_text(
        "CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id));"
    )

    manager = SchemaManager(db_pool, schema_dir=str(schema_dir))

    await manager.apply_schema()

    # Verify both tables exist
    async with db_pool.acquire() as conn:
        users_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')"
        )
        posts_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'posts')"
        )
        assert users_exists is True
        assert posts_exists is True


@pytest.mark.asyncio
async def test_apply_schema_with_no_files(db_pool, tmp_path):
    """Should handle empty schema directory gracefully."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    manager = SchemaManager(db_pool, schema_dir=str(schema_dir))

    # Should not raise, just log warning
    await manager.apply_schema()


@pytest.mark.asyncio
async def test_apply_schema_with_invalid_directory(db_pool):
    """Should raise SchemaError for non-existent directory."""
    manager = SchemaManager(db_pool, schema_dir="/nonexistent/path")

    with pytest.raises(SchemaError, match="Schema directory not found"):
        await manager.apply_schema()
