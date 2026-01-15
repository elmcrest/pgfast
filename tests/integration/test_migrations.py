"""Integration tests for migration system."""

import pytest

from pgfast.exceptions import ChecksumError, DependencyError, MigrationError


async def test_ensure_migrations_table(manager):
    """Test migration tracking table creation."""
    await manager._ensure_migrations_table()

    async with manager.pool.acquire() as conn:
        # Check table exists
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = '_pgfast_migrations'"
        )
        assert result == 1

        # Verify schema includes checksum column
        result = await conn.fetchval(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = '_pgfast_migrations' AND column_name = 'checksum'
            """
        )
        assert result == 1


async def test_create_migration(manager):
    """Test migration file creation."""
    # Get target directory from manager's config
    target_dir = manager.migrations_dirs[0]
    up_file, down_file = manager.create_migration("add_users_table", target_dir)

    assert up_file.exists()
    assert down_file.exists()
    assert "_up.sql" in up_file.name
    assert "_down.sql" in down_file.name
    assert "add_users_table" in up_file.name

    # Check content
    up_content = up_file.read_text()
    assert "Migration:" in up_content
    assert "add_users_table" in up_content

    down_content = down_file.read_text()
    assert "Migration:" in down_content
    assert "rollback" in down_content


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


async def test_schema_up(manager, tmp_path):
    """Test applying migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create a simple migration
    up_sql = "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT);"
    down_sql = "DROP TABLE test_users;"

    (migrations_dir / "20250101000000_create_test_users_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_create_test_users_down.sql").write_text(down_sql)

    # Apply migration
    applied = await manager.schema_up()

    assert applied == [20250101000000]

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_users'"
        )
        assert result == 1

    # Verify tracking table includes checksum
    current_version = await manager.get_current_version()
    assert current_version == 20250101000000

    checksums = await manager.get_migration_checksums()
    assert 20250101000000 in checksums
    assert len(checksums[20250101000000]) == 64  # SHA-256 hex length


async def test_schema_down(manager, tmp_path):
    """Test rolling back migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply migration
    up_sql = "CREATE TABLE test_posts (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_posts;"

    (migrations_dir / "20250101000000_create_posts_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_create_posts_down.sql").write_text(down_sql)

    await manager.schema_up()

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_posts'"
        )
        assert result == 1

    # Rollback
    rolled_back = await manager.schema_down()

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
    await manager.schema_up(target=20250101000000)

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
        await manager.schema_up()

    # Verify nothing was recorded (transaction rollback)
    current_version = await manager.get_current_version()
    assert current_version == 0


# ==================== New Feature Tests ====================


async def test_dependency_tracking(manager, tmp_path):
    """Test migration dependency tracking and validation."""
    migrations_dir = tmp_path / "migrations"

    # Create migrations with dependencies
    # Migration A - no dependencies
    up_a = "CREATE TABLE table_a (id SERIAL PRIMARY KEY);"
    down_a = "DROP TABLE table_a;"
    (migrations_dir / "20250101000000_create_a_up.sql").write_text(up_a)
    (migrations_dir / "20250101000000_create_a_down.sql").write_text(down_a)

    # Migration B - depends on A
    up_b = "-- depends_on: 20250101000000\nCREATE TABLE table_b (id SERIAL PRIMARY KEY, a_id INTEGER REFERENCES table_a(id));"
    down_b = "DROP TABLE table_b;"
    (migrations_dir / "20250102000000_create_b_up.sql").write_text(up_b)
    (migrations_dir / "20250102000000_create_b_down.sql").write_text(down_b)

    # Get dependency graph
    dep_graph = manager.get_dependency_graph()
    assert 20250101000000 in dep_graph
    assert 20250102000000 in dep_graph
    assert dep_graph[20250101000000] == []
    assert dep_graph[20250102000000] == [20250101000000]

    # Apply migrations - should be sorted by dependency order
    applied = await manager.schema_up()
    assert applied == [20250101000000, 20250102000000]

    # Verify both tables exist
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename IN ('table_a', 'table_b')"
        )
        assert result == 2


async def test_dependency_validation_fails_on_missing_dependency(manager, tmp_path):
    """Test that migration fails if dependency is not applied."""
    migrations_dir = tmp_path / "migrations"

    # Create migration that depends on non-existent migration
    up_sql = "-- depends_on: 20240101000000\nCREATE TABLE test_table (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_table;"
    (migrations_dir / "20250101000000_with_missing_dep_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_with_missing_dep_down.sql").write_text(down_sql)

    # Should raise DependencyError
    with pytest.raises(DependencyError) as exc_info:
        await manager.schema_up()

    assert "depends on unknown migration" in str(exc_info.value)


async def test_circular_dependency_detection(manager, tmp_path):
    """Test circular dependency detection."""
    migrations_dir = tmp_path / "migrations"

    # Create migrations with circular dependencies
    # A depends on B
    up_a = (
        "-- depends_on: 20250102000000\nCREATE TABLE table_a (id SERIAL PRIMARY KEY);"
    )
    down_a = "DROP TABLE table_a;"
    (migrations_dir / "20250101000000_create_a_up.sql").write_text(up_a)
    (migrations_dir / "20250101000000_create_a_down.sql").write_text(down_a)

    # B depends on A (circular!)
    up_b = (
        "-- depends_on: 20250101000000\nCREATE TABLE table_b (id SERIAL PRIMARY KEY);"
    )
    down_b = "DROP TABLE table_b;"
    (migrations_dir / "20250102000000_create_b_up.sql").write_text(up_b)
    (migrations_dir / "20250102000000_create_b_down.sql").write_text(down_b)

    # Should raise DependencyError due to circular dependency
    with pytest.raises(DependencyError) as exc_info:
        await manager.schema_up()

    assert "Circular dependency" in str(exc_info.value)


async def test_dependency_prevents_rollback(manager, tmp_path):
    """Test that rollback works correctly with dependencies."""
    migrations_dir = tmp_path / "migrations"

    # Create three migrations: A, B depends on A, C depends on A
    up_a = "CREATE TABLE table_a (id SERIAL PRIMARY KEY);"
    down_a = "DROP TABLE table_a;"
    (migrations_dir / "20250101000000_create_a_up.sql").write_text(up_a)
    (migrations_dir / "20250101000000_create_a_down.sql").write_text(down_a)

    up_b = "-- depends_on: 20250101000000\nCREATE TABLE table_b (id SERIAL PRIMARY KEY, a_id INTEGER REFERENCES table_a(id));"
    down_b = "DROP TABLE table_b;"
    (migrations_dir / "20250102000000_create_b_up.sql").write_text(up_b)
    (migrations_dir / "20250102000000_create_b_down.sql").write_text(down_b)

    up_c = (
        "-- depends_on: 20250101000000\nCREATE TABLE table_c (id SERIAL PRIMARY KEY);"
    )
    down_c = "DROP TABLE table_c;"
    (migrations_dir / "20250103000000_create_c_up.sql").write_text(up_c)
    (migrations_dir / "20250103000000_create_c_down.sql").write_text(down_c)

    # Apply all three
    await manager.schema_up()

    # Rollback B and C (target=20250101000000), leaving A
    # This should work fine - rolling back dependent migrations while keeping their dependency
    rolled_back = await manager.schema_down(target=20250101000000)
    assert set(rolled_back) == {20250102000000, 20250103000000}

    current_version = await manager.get_current_version()
    assert current_version == 20250101000000

    # Rolling back A should also work (no remaining dependencies)
    await manager.schema_down(target=0)

    current_version = await manager.get_current_version()
    assert current_version == 0


async def test_checksum_validation(manager, tmp_path):
    """Test checksum calculation and validation."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply migration
    up_sql = "CREATE TABLE test_table (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_table;"

    up_file = migrations_dir / "20250101000000_test_up.sql"
    down_file = migrations_dir / "20250101000000_test_down.sql"

    up_file.write_text(up_sql)
    down_file.write_text(down_sql)

    # Apply migration
    await manager.schema_up()

    # Verify checksums
    results = await manager.verify_checksums()
    assert len(results["valid"]) == 1
    assert len(results["invalid"]) == 0
    assert "20250101000000" in results["valid"][0]

    # Modify the migration file
    up_file.write_text(up_sql + "\n-- Modified!")

    # Verify checksums again - should detect modification
    results = await manager.verify_checksums()
    assert len(results["valid"]) == 0
    assert len(results["invalid"]) == 1
    assert "CHECKSUM MISMATCH" in results["invalid"][0]


async def test_checksum_validation_prevents_migration(manager, tmp_path):
    """Test that checksum validation prevents running migrations with modified files."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply first migration
    up_sql_1 = "CREATE TABLE table1 (id SERIAL PRIMARY KEY);"
    down_sql_1 = "DROP TABLE table1;"
    up_file_1 = migrations_dir / "20250101000000_first_up.sql"
    down_file_1 = migrations_dir / "20250101000000_first_down.sql"
    up_file_1.write_text(up_sql_1)
    down_file_1.write_text(down_sql_1)

    await manager.schema_up()

    # Modify the applied migration
    up_file_1.write_text(up_sql_1 + "\n-- Modified!")

    # Create a second migration
    up_sql_2 = "CREATE TABLE table2 (id SERIAL PRIMARY KEY);"
    down_sql_2 = "DROP TABLE table2;"
    (migrations_dir / "20250102000000_second_up.sql").write_text(up_sql_2)
    (migrations_dir / "20250102000000_second_down.sql").write_text(down_sql_2)

    # Try to apply second migration - should fail due to checksum mismatch
    with pytest.raises(ChecksumError) as exc_info:
        await manager.schema_up()

    assert "Checksum validation failed" in str(exc_info.value)
    assert "has been modified" in str(exc_info.value)


async def test_checksum_validation_with_force_flag(manager, tmp_path):
    """Test that --force flag bypasses checksum validation."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply first migration
    up_sql_1 = "CREATE TABLE table1 (id SERIAL PRIMARY KEY);"
    down_sql_1 = "DROP TABLE table1;"
    up_file_1 = migrations_dir / "20250101000000_first_up.sql"
    down_file_1 = migrations_dir / "20250101000000_first_down.sql"
    up_file_1.write_text(up_sql_1)
    down_file_1.write_text(down_sql_1)

    await manager.schema_up()

    # Modify the applied migration
    up_file_1.write_text(up_sql_1 + "\n-- Modified!")

    # Create a second migration
    up_sql_2 = "CREATE TABLE table2 (id SERIAL PRIMARY KEY);"
    down_sql_2 = "DROP TABLE table2;"
    (migrations_dir / "20250102000000_second_up.sql").write_text(up_sql_2)
    (migrations_dir / "20250102000000_second_down.sql").write_text(down_sql_2)

    # Apply with force=True should work
    applied = await manager.schema_up(force=True)
    assert 20250102000000 in applied

    # Verify second table was created
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'table2'"
        )
        assert result == 1


async def test_dry_run_schema_up(manager, tmp_path):
    """Test dry-run mode for schema up."""
    migrations_dir = tmp_path / "migrations"

    # Create migrations
    up_sql = "CREATE TABLE test_table (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_table;"
    (migrations_dir / "20250101000000_test_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_test_down.sql").write_text(down_sql)

    # Run in dry-run mode
    applied = await manager.schema_up(dry_run=True)

    # Should return what would be applied
    assert applied == [20250101000000]

    # But table should NOT exist
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_table'"
        )
        assert result == 0

    # And migration should NOT be recorded
    current_version = await manager.get_current_version()
    assert current_version == 0

    # Now apply for real
    await manager.schema_up(dry_run=False)

    # Table should now exist
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_table'"
        )
        assert result == 1


async def test_dry_run_schema_down(manager, tmp_path):
    """Test dry-run mode for schema down."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply migration
    up_sql = "CREATE TABLE test_table (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_table;"
    (migrations_dir / "20250101000000_test_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_test_down.sql").write_text(down_sql)

    await manager.schema_up()

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_table'"
        )
        assert result == 1

    # Run rollback in dry-run mode
    rolled_back = await manager.schema_down(dry_run=True)

    # Should return what would be rolled back
    assert rolled_back == [20250101000000]

    # But table should STILL exist
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_table'"
        )
        assert result == 1

    # And migration should still be recorded
    current_version = await manager.get_current_version()
    assert current_version == 20250101000000

    # Now rollback for real
    await manager.schema_down(dry_run=False)

    # Table should be gone
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_table'"
        )
        assert result == 0


async def test_preview_migration(manager, tmp_path):
    """Test migration preview functionality."""
    migrations_dir = tmp_path / "migrations"

    # Create migration with multi-line SQL
    up_sql = """-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
"""
    down_sql = "DROP TABLE users;"

    up_file = migrations_dir / "20250101000000_create_users_up.sql"
    down_file = migrations_dir / "20250101000000_create_users_down.sql"
    up_file.write_text(up_sql)
    down_file.write_text(down_sql)

    # Discover migration
    migrations = manager._discover_migrations()
    assert len(migrations) == 1

    migration = migrations[0]

    # Get preview
    preview = manager.preview_migration(migration, "up")

    assert preview["version"] == 20250101000000
    assert preview["name"] == "create_users"
    assert preview["dependencies"] == []
    assert len(preview["checksum"]) == 64
    assert "CREATE TABLE users" in preview["sql_preview"]
    assert preview["total_lines"] > 0


async def test_topological_sort_complex_dependencies(manager, tmp_path):
    """Test topological sort with complex dependency graph."""
    migrations_dir = tmp_path / "migrations"

    # Create complex dependency graph:
    # A -> no deps
    # B -> depends on A
    # C -> depends on A
    # D -> depends on B and C

    up_a = "CREATE TABLE table_a (id SERIAL PRIMARY KEY);"
    down_a = "DROP TABLE table_a;"
    (migrations_dir / "20250101000000_a_up.sql").write_text(up_a)
    (migrations_dir / "20250101000000_a_down.sql").write_text(down_a)

    up_b = (
        "-- depends_on: 20250101000000\nCREATE TABLE table_b (id SERIAL PRIMARY KEY);"
    )
    down_b = "DROP TABLE table_b;"
    (migrations_dir / "20250102000000_b_up.sql").write_text(up_b)
    (migrations_dir / "20250102000000_b_down.sql").write_text(down_b)

    up_c = (
        "-- depends_on: 20250101000000\nCREATE TABLE table_c (id SERIAL PRIMARY KEY);"
    )
    down_c = "DROP TABLE table_c;"
    (migrations_dir / "20250103000000_c_up.sql").write_text(up_c)
    (migrations_dir / "20250103000000_c_down.sql").write_text(down_c)

    up_d = "-- depends_on: 20250102000000, 20250103000000\nCREATE TABLE table_d (id SERIAL PRIMARY KEY);"
    down_d = "DROP TABLE table_d;"
    (migrations_dir / "20250104000000_d_up.sql").write_text(up_d)
    (migrations_dir / "20250104000000_d_down.sql").write_text(down_d)

    # Apply all migrations
    applied = await manager.schema_up()

    # Verify correct order: A must be first, D must be last, B and C in between
    assert applied[0] == 20250101000000  # A first
    assert applied[3] == 20250104000000  # D last
    assert 20250102000000 in applied  # B somewhere
    assert 20250103000000 in applied  # C somewhere

    # Verify all tables exist
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'table_%'"
        )
        assert result == 4


async def test_auto_dependency_creation(manager, tmp_path):
    """Test that new migrations automatically depend on the latest existing migration."""
    # Use the migrations directory already configured in the manager
    migrations_dir = manager.migrations_dirs[0]

    # Create first migration (should have no dependencies)
    up1, down1 = manager.create_migration("first_migration", migrations_dir)
    content1 = up1.read_text()

    assert "depends_on:" not in content1

    # Create second migration (should auto-depend on first)
    up2, down2 = manager.create_migration("second_migration", migrations_dir)
    content2 = up2.read_text()

    assert "depends_on:" in content2
    # Extract first migration's version from filename
    first_version = up1.stem.split("_")[0]
    assert first_version in content2

    # Create third migration with auto_depend=False (should have no dependencies)
    up3, down3 = manager.create_migration(
        "third_migration", migrations_dir, auto_depend=False
    )
    content3 = up3.read_text()

    assert "depends_on:" not in content3

    # Create fourth migration (should auto-depend on third, the latest)
    up4, down4 = manager.create_migration("fourth_migration", migrations_dir)
    content4 = up4.read_text()

    assert "depends_on:" in content4
    # Should depend on third migration (the latest)
    third_version = up3.stem.split("_")[0]
    assert third_version in content4


async def test_schema_up_progress_callback(manager, tmp_path):
    """Test that schema_up calls progress callback with correct parameters."""
    migrations_dir = tmp_path / "migrations"

    # Create two migrations
    (migrations_dir / "20250101000000_first_up.sql").write_text(
        "CREATE TABLE callback_test_1 (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250101000000_first_down.sql").write_text(
        "DROP TABLE callback_test_1;"
    )
    (migrations_dir / "20250102000000_second_up.sql").write_text(
        "-- depends_on: 20250101000000\nCREATE TABLE callback_test_2 (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250102000000_second_down.sql").write_text(
        "DROP TABLE callback_test_2;"
    )

    # Track callback invocations
    calls = []

    def on_progress(migration, current, total, status, elapsed):
        calls.append(
            {
                "version": migration.version,
                "name": migration.name,
                "current": current,
                "total": total,
                "status": status,
                "elapsed": elapsed,
            }
        )

    # Apply migrations with callback
    applied = await manager.schema_up(on_progress=on_progress)

    assert applied == [20250101000000, 20250102000000]

    # Verify callback was called correctly
    assert len(calls) == 4  # 2 migrations × 2 statuses (started + completed)

    # First migration: started
    assert calls[0]["version"] == 20250101000000
    assert calls[0]["current"] == 1
    assert calls[0]["total"] == 2
    assert calls[0]["status"] == "started"
    assert calls[0]["elapsed"] == 0.0

    # First migration: completed
    assert calls[1]["version"] == 20250101000000
    assert calls[1]["current"] == 1
    assert calls[1]["total"] == 2
    assert calls[1]["status"] == "completed"
    assert calls[1]["elapsed"] > 0  # Should have some elapsed time

    # Second migration: started
    assert calls[2]["version"] == 20250102000000
    assert calls[2]["current"] == 2
    assert calls[2]["total"] == 2
    assert calls[2]["status"] == "started"

    # Second migration: completed
    assert calls[3]["version"] == 20250102000000
    assert calls[3]["current"] == 2
    assert calls[3]["total"] == 2
    assert calls[3]["status"] == "completed"
    assert calls[3]["elapsed"] > 0


async def test_schema_down_progress_callback(manager, tmp_path):
    """Test that schema_down calls progress callback with correct parameters."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply two migrations
    (migrations_dir / "20250101000000_first_up.sql").write_text(
        "CREATE TABLE rollback_test_1 (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250101000000_first_down.sql").write_text(
        "DROP TABLE rollback_test_1;"
    )
    (migrations_dir / "20250102000000_second_up.sql").write_text(
        "CREATE TABLE rollback_test_2 (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250102000000_second_down.sql").write_text(
        "DROP TABLE rollback_test_2;"
    )

    # Apply both migrations first
    await manager.schema_up()

    # Track callback invocations for rollback
    calls = []

    def on_progress(migration, current, total, status, elapsed):
        calls.append(
            {
                "version": migration.version,
                "current": current,
                "total": total,
                "status": status,
                "elapsed": elapsed,
            }
        )

    # Rollback both migrations
    rolled_back = await manager.schema_down(steps=2, on_progress=on_progress)

    assert set(rolled_back) == {20250101000000, 20250102000000}

    # Verify callback was called correctly
    assert len(calls) == 4  # 2 migrations × 2 statuses

    # Rollback happens in reverse order (newest first)
    assert calls[0]["version"] == 20250102000000
    assert calls[0]["current"] == 1
    assert calls[0]["total"] == 2
    assert calls[0]["status"] == "started"

    assert calls[1]["version"] == 20250102000000
    assert calls[1]["status"] == "completed"
    assert calls[1]["elapsed"] > 0

    assert calls[2]["version"] == 20250101000000
    assert calls[2]["current"] == 2
    assert calls[2]["total"] == 2
    assert calls[2]["status"] == "started"

    assert calls[3]["version"] == 20250101000000
    assert calls[3]["status"] == "completed"


async def test_schema_up_no_callback(manager, tmp_path):
    """Test that schema_up works fine without a callback (default behavior)."""
    migrations_dir = tmp_path / "migrations"

    (migrations_dir / "20250101000000_no_callback_up.sql").write_text(
        "CREATE TABLE no_callback_test (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250101000000_no_callback_down.sql").write_text(
        "DROP TABLE no_callback_test;"
    )

    # Apply without callback (on_progress=None is default)
    applied = await manager.schema_up()

    assert applied == [20250101000000]


async def test_schema_up_with_timeout(manager, tmp_path):
    """Test that schema_up accepts and uses timeout parameter."""
    migrations_dir = tmp_path / "migrations"

    # Create a simple migration
    up_sql = "CREATE TABLE test_timeout_up (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_timeout_up;"

    (migrations_dir / "20250101000000_test_timeout_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_test_timeout_down.sql").write_text(down_sql)

    # Apply migration with explicit timeout (should work fine for quick migration)
    applied = await manager.schema_up(timeout=30.0)

    assert applied == [20250101000000]

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_timeout_up'"
        )
        assert result == 1


async def test_schema_down_with_timeout(manager, tmp_path):
    """Test that schema_down accepts and uses timeout parameter."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply migration
    up_sql = "CREATE TABLE test_timeout_down (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_timeout_down;"

    (migrations_dir / "20250101000000_test_timeout_down_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_test_timeout_down_down.sql").write_text(down_sql)

    await manager.schema_up()

    # Rollback with explicit timeout
    rolled_back = await manager.schema_down(timeout=30.0)

    assert rolled_back == [20250101000000]

    # Verify table is gone
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_timeout_down'"
        )
        assert result == 0


async def test_schema_up_with_no_timeout(manager, tmp_path):
    """Test that schema_up with timeout=None (default) works correctly."""
    migrations_dir = tmp_path / "migrations"

    # Create a simple migration
    up_sql = "CREATE TABLE test_no_timeout (id SERIAL PRIMARY KEY);"
    down_sql = "DROP TABLE test_no_timeout;"

    (migrations_dir / "20250101000000_test_no_timeout_up.sql").write_text(up_sql)
    (migrations_dir / "20250101000000_test_no_timeout_down.sql").write_text(down_sql)

    # Apply migration with default timeout=None (no limit)
    applied = await manager.schema_up(timeout=None)

    assert applied == [20250101000000]

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_no_timeout'"
        )
        assert result == 1


async def test_discover_migrations_in_subdirectories(manager, tmp_path):
    """Test that migrations in subdirectories are properly discovered.

    This tests the structure commonly used in larger projects where migrations
    are organized by domain/module:

    migrations/
    ├── tenants/
    │   ├── 20251117195818785_create_down.sql
    │   └── 20251117195818785_create_up.sql
    ├── locations/
    │   ├── 20251117195827054_create_down.sql
    │   └── 20251117195827054_create_up.sql
    └── appointments/
        ├── 20251121172724778_appointments_model_down.sql
        └── 20251121172724778_appointments_model_up.sql
    """
    migrations_dir = tmp_path / "migrations"

    # Create subdirectories mimicking a real project structure
    tenants_dir = migrations_dir / "tenants"
    locations_dir = migrations_dir / "locations"
    appointments_dir = migrations_dir / "appointments"

    tenants_dir.mkdir(parents=True)
    locations_dir.mkdir(parents=True)
    appointments_dir.mkdir(parents=True)

    # Create migrations in subdirectories (tenants first - no deps)
    (tenants_dir / "20251117195818785_create_up.sql").write_text(
        "CREATE TABLE tenants (id SERIAL PRIMARY KEY, name TEXT);"
    )
    (tenants_dir / "20251117195818785_create_down.sql").write_text(
        "DROP TABLE tenants;"
    )

    # Locations depends on tenants
    (locations_dir / "20251117195827054_create_up.sql").write_text(
        "-- depends_on: 20251117195818785\n"
        "CREATE TABLE locations (id SERIAL PRIMARY KEY, tenant_id INTEGER REFERENCES tenants(id));"
    )
    (locations_dir / "20251117195827054_create_down.sql").write_text(
        "DROP TABLE locations;"
    )

    # Appointments depends on locations
    (appointments_dir / "20251121172724778_appointments_model_up.sql").write_text(
        "-- depends_on: 20251117195827054\n"
        "CREATE TABLE appointments (id SERIAL PRIMARY KEY, location_id INTEGER REFERENCES locations(id));"
    )
    (appointments_dir / "20251121172724778_appointments_model_down.sql").write_text(
        "DROP TABLE appointments;"
    )

    # Discover migrations - should find all 3 from subdirectories
    migrations = manager._discover_migrations()

    assert len(migrations) == 3, f"Expected 3 migrations, found {len(migrations)}"

    # Verify versions are correct
    versions = {m.version for m in migrations}
    assert versions == {20251117195818785, 20251117195827054, 20251121172724778}

    # Verify dependencies are parsed correctly
    migration_map = {m.version: m for m in migrations}
    assert migration_map[20251117195818785].dependencies == []
    assert migration_map[20251117195827054].dependencies == [20251117195818785]
    assert migration_map[20251121172724778].dependencies == [20251117195827054]

    # Apply migrations - should respect dependency order
    applied = await manager.schema_up()

    assert applied == [20251117195818785, 20251117195827054, 20251121172724778]

    # Verify all tables exist
    async with manager.pool.acquire() as conn:
        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('tenants', 'locations', 'appointments')"
        )
        table_names = {row["tablename"] for row in tables}
        assert table_names == {"tenants", "locations", "appointments"}


# ==================== Python Migration Integration Tests ====================


async def test_python_migration_discovery(manager, tmp_path):
    """Test that Python migrations are discovered alongside SQL migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create an SQL migration
    (migrations_dir / "20250101000000_create_table_up.sql").write_text(
        "CREATE TABLE sql_table (id SERIAL PRIMARY KEY);"
    )
    (migrations_dir / "20250101000000_create_table_down.sql").write_text(
        "DROP TABLE sql_table;"
    )

    # Create a Python migration
    (migrations_dir / "20250102000000_populate_data_up.py").write_text("""
# depends_on: 20250101000000

async def migrate(conn):
    await conn.execute("INSERT INTO sql_table DEFAULT VALUES")
""")
    (migrations_dir / "20250102000000_populate_data_down.py").write_text("""
async def migrate(conn):
    await conn.execute("DELETE FROM sql_table")
""")

    # Discover migrations
    migrations = manager._discover_migrations()

    assert len(migrations) == 2

    sql_migration = next(m for m in migrations if m.migration_type == "sql")
    py_migration = next(m for m in migrations if m.migration_type == "python")

    assert sql_migration.version == 20250101000000
    assert sql_migration.name == "create_table"

    assert py_migration.version == 20250102000000
    assert py_migration.name == "populate_data"
    assert py_migration.dependencies == [20250101000000]


async def test_python_migration_schema_up(manager, tmp_path):
    """Test applying Python migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create a Python migration that creates a table
    (migrations_dir / "20250101000000_create_py_table_up.py").write_text("""
async def migrate(conn):
    await conn.execute('''
        CREATE TABLE python_created_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            created_by TEXT DEFAULT 'python_migration'
        )
    ''')
""")
    (migrations_dir / "20250101000000_create_py_table_down.py").write_text("""
async def migrate(conn):
    await conn.execute("DROP TABLE python_created_table")
""")

    # Apply migration
    applied = await manager.schema_up()

    assert applied == [20250101000000]

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'python_created_table'"
        )
        assert result == 1

        # Verify table has expected columns
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'python_created_table'"
        )
        column_names = {row["column_name"] for row in columns}
        assert column_names == {"id", "name", "created_by"}


async def test_python_migration_schema_down(manager, tmp_path):
    """Test rolling back Python migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create a Python migration
    (migrations_dir / "20250101000000_create_py_rollback_up.py").write_text("""
async def migrate(conn):
    await conn.execute("CREATE TABLE py_rollback_test (id SERIAL PRIMARY KEY)")
""")
    (migrations_dir / "20250101000000_create_py_rollback_down.py").write_text("""
async def migrate(conn):
    await conn.execute("DROP TABLE py_rollback_test")
""")

    # Apply migration
    await manager.schema_up()

    # Verify table exists
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'py_rollback_test'"
        )
        assert result == 1

    # Rollback
    rolled_back = await manager.schema_down()

    assert rolled_back == [20250101000000]

    # Verify table is gone
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'py_rollback_test'"
        )
        assert result == 0


async def test_mixed_sql_and_python_migrations(manager, tmp_path):
    """Test applying mixed SQL and Python migrations with dependencies."""
    migrations_dir = tmp_path / "migrations"

    # SQL migration 1: Create users table
    (migrations_dir / "20250101000000_create_users_up.sql").write_text(
        "CREATE TABLE mixed_users (id SERIAL PRIMARY KEY, name TEXT);"
    )
    (migrations_dir / "20250101000000_create_users_down.sql").write_text(
        "DROP TABLE mixed_users;"
    )

    # Python migration 2: Populate initial data (depends on SQL migration)
    (migrations_dir / "20250102000000_populate_users_up.py").write_text("""
# depends_on: 20250101000000

async def migrate(conn):
    await conn.execute("INSERT INTO mixed_users (name) VALUES ('Alice'), ('Bob')")
""")
    (migrations_dir / "20250102000000_populate_users_down.py").write_text("""
async def migrate(conn):
    await conn.execute("DELETE FROM mixed_users WHERE name IN ('Alice', 'Bob')")
""")

    # SQL migration 3: Add email column (depends on Python migration)
    (migrations_dir / "20250103000000_add_email_up.sql").write_text(
        "-- depends_on: 20250102000000\nALTER TABLE mixed_users ADD COLUMN email TEXT;"
    )
    (migrations_dir / "20250103000000_add_email_down.sql").write_text(
        "ALTER TABLE mixed_users DROP COLUMN email;"
    )

    # Apply all migrations
    applied = await manager.schema_up()

    assert applied == [20250101000000, 20250102000000, 20250103000000]

    # Verify table exists with data and email column
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval("SELECT COUNT(*) FROM mixed_users")
        assert result == 2

        # Verify email column exists
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'mixed_users'"
        )
        column_names = {row["column_name"] for row in columns}
        assert "email" in column_names


async def test_python_migration_error_handling(manager, tmp_path):
    """Test error handling in Python migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create a Python migration with invalid SQL
    (migrations_dir / "20250101000000_bad_python_up.py").write_text("""
async def migrate(conn):
    await conn.execute("THIS IS NOT VALID SQL")
""")
    (migrations_dir / "20250101000000_bad_python_down.py").write_text("""
async def migrate(conn):
    pass
""")

    # Should raise MigrationError
    with pytest.raises(MigrationError):
        await manager.schema_up()

    # Verify nothing was recorded (transaction rollback)
    current_version = await manager.get_current_version()
    assert current_version == 0


async def test_python_migration_with_complex_logic(manager, tmp_path):
    """Test Python migration with complex business logic."""
    migrations_dir = tmp_path / "migrations"

    # First create a table with SQL
    (migrations_dir / "20250101000000_create_products_up.sql").write_text(
        "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price NUMERIC);"
    )
    (migrations_dir / "20250101000000_create_products_down.sql").write_text(
        "DROP TABLE products;"
    )

    # Python migration that does complex data manipulation
    (migrations_dir / "20250102000000_seed_products_up.py").write_text("""
# depends_on: 20250101000000
import hashlib

async def migrate(conn):
    # Complex logic that would be hard to express in SQL
    products = [
        ('Widget', 9.99),
        ('Gadget', 19.99),
        ('Thingamajig', 29.99),
    ]

    for name, base_price in products:
        # Calculate discount based on name hash (silly example, but shows Python logic)
        name_hash = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
        discount = (name_hash % 10) / 100  # 0-9% discount
        final_price = round(base_price * (1 - discount), 2)

        await conn.execute(
            "INSERT INTO products (name, price) VALUES ($1, $2)",
            name, final_price
        )
""")
    (migrations_dir / "20250102000000_seed_products_down.py").write_text("""
async def migrate(conn):
    await conn.execute("DELETE FROM products WHERE name IN ('Widget', 'Gadget', 'Thingamajig')")
""")

    # Apply migrations
    applied = await manager.schema_up()

    assert applied == [20250101000000, 20250102000000]

    # Verify products were created
    async with manager.pool.acquire() as conn:
        result = await conn.fetchval("SELECT COUNT(*) FROM products")
        assert result == 3


async def test_create_python_migration(manager):
    """Test creating Python migration files via SchemaManager."""
    target_dir = manager.migrations_dirs[0]

    up_file, down_file = manager.create_migration(
        "test_python_creation", target_dir, python=True
    )

    assert up_file.exists()
    assert down_file.exists()
    assert "_up.py" in up_file.name
    assert "_down.py" in down_file.name
    assert "test_python_creation" in up_file.name

    # Check content
    up_content = up_file.read_text()
    assert "async def migrate(conn" in up_content
    assert "asyncpg.Connection" in up_content
    assert "Migration: test_python_creation" in up_content

    down_content = down_file.read_text()
    assert "async def migrate(conn" in down_content
    assert "rollback" in down_content


async def test_python_migration_auto_dependency(manager):
    """Test that Python migrations auto-depend on latest migration."""
    target_dir = manager.migrations_dirs[0]

    # Create first SQL migration
    up1, down1 = manager.create_migration("first_sql", target_dir)

    # Create Python migration (should depend on SQL migration)
    up2, down2 = manager.create_migration("second_python", target_dir, python=True)

    content2 = up2.read_text()
    first_version = up1.stem.split("_")[0]

    assert "# depends_on:" in content2
    assert first_version in content2


async def test_python_migration_checksum_validation(manager, tmp_path):
    """Test checksum validation for Python migrations."""
    migrations_dir = tmp_path / "migrations"

    # Create and apply Python migration
    up_file = migrations_dir / "20250101000000_py_checksum_up.py"
    down_file = migrations_dir / "20250101000000_py_checksum_down.py"

    up_file.write_text("""
async def migrate(conn):
    await conn.execute("CREATE TABLE py_checksum_test (id SERIAL PRIMARY KEY)")
""")
    down_file.write_text("""
async def migrate(conn):
    await conn.execute("DROP TABLE py_checksum_test")
""")

    # Apply migration
    await manager.schema_up()

    # Verify checksums
    results = await manager.verify_checksums()
    assert len(results["valid"]) == 1
    assert len(results["invalid"]) == 0
    assert "20250101000000" in results["valid"][0]

    # Modify the Python migration file
    up_file.write_text("""
async def migrate(conn):
    # Modified!
    await conn.execute("CREATE TABLE py_checksum_test (id SERIAL PRIMARY KEY)")
""")

    # Verify checksums again - should detect modification
    results = await manager.verify_checksums()
    assert len(results["valid"]) == 0
    assert len(results["invalid"]) == 1
    assert "CHECKSUM MISMATCH" in results["invalid"][0]
