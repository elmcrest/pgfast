"""Integration tests for migration system."""

import pytest

from pgfast.exceptions import MigrationError


async def test_ensure_migrations_table(manager):
    """Test migration tracking table creation."""
    await manager._ensure_migrations_table()

    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = '_pgfast_migrations'"
        )
        assert result == 1


async def test_create_migration(manager):
    """Test migration file creation."""
    up_file, down_file = manager.create_migration("add_users_table")

    assert up_file.exists()
    assert down_file.exists()
    assert "_up.sql" in up_file.name
    assert "_down.sql" in down_file.name
    assert "add_users_table" in up_file.name

    # Check content
    content = up_file.read_text()
    assert "Migration:" in content
    assert "add_users_table" in content


async def test_discover_migrations(manager, tmp_path):
    """Test migration discovery."""
    migrations_dir = tmp_path / "migrations"

    # Create test migrations
    (migrations_dir / "20250101000000_first_up.sql").write_text("SELECT 1;")
    (migrations_dir / "20250101000000_first_down.sql").write_text("SELECT 2;")
    (migrations_dir / "20250102000000_second_up.sql").write_text("SELECT 3;")
    (migrations_dir / "20250102000000_second_down.sql").write_text("SELECT 4;")

    migrations = manager._discover_migrations()

    assert len(migrations) == 2
    assert migrations[0].version == 20250101000000
    assert migrations[0].name == "first"
    assert migrations[1].version == 20250102000000
    assert migrations[1].name == "second"


async def test_migrate_up(manager, tmp_path):
    """Test applying migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create a simple migration
    up_sql = "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT);"
    down_sql = "DROP TABLE test_users;"

    (migrations_dir / "20250101000000_create_test_users_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_create_test_users_down.sql").write_text(down_sql)

    # Apply migration
    applied = await manager.migrate_up()

    assert applied == [20250101000000]

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_users'"
        )
        assert result == 1

    # Verify tracking table
    current_version = await manager.get_current_version()
    assert current_version == 20250101000000


async def test_migrate_down(manager, tmp_path):
    """Test rolling back migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply migration
    up_sql = "CREATE TABLE test_posts (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_posts;"

    (migrations_dir / "20250101000000_create_posts_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_create_posts_down.sql").write_text(down_sql)

    await manager.migrate_up()

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_posts'"
        )
        assert result == 1

    # Rollback
    rolled_back = await manager.migrate_down()

    assert rolled_back == [20250101000000]

    # Verify table is gone
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_posts'"
        )
        assert result == 0

    # Verify tracking table
    current_version = await manager.get_current_version()
    assert current_version == 0


async def test_pending_migrations(manager, tmp_path):
    """Test getting pending migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create two migrations
    (migrations_dir / "20250101000000_first_up.sql").write_text("SELECT 1;")
    (migrations_dir / "20250101000000_first_down.sql").write_text("SELECT 2;")
    (migrations_dir / "20250102000000_second_up.sql").write_text("SELECT 3;")
    (migrations_dir / "20250102000000_second_down.sql").write_text("SELECT 4;")

    # Initially both pending
    pending = await manager.get_pending_migrations()
    assert len(pending) == 2

    # Apply first
    await manager.migrate_up(target=20250101000000)

    # Now only second is pending
    pending = await manager.get_pending_migrations()
    assert len(pending) == 1
    assert pending[0].version == 20250102000000


async def test_migration_error_handling(manager, tmp_path):
    """Test error handling during migration."""
    migrations_dir = tmp_path / "migrations"

    # Create migration with invalid SQL
    bad_sql = "THIS IS NOT VALID SQL;"
    (migrations_dir / "20250101000000_bad_migration_up.sql").write_text(bad_sql)
    (migrations_dir / "20250101000000_bad_migration_down.sql").write_text("SELECT 1;")

    # Should raise MigrationError
    with pytest.raises(MigrationError):
        await manager.migrate_up()

    # Verify nothing was recorded
    current_version = await manager.get_current_version()
    assert current_version == 0
