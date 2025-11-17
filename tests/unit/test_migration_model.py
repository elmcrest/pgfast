"""Unit tests for Migration model."""

from pgfast.migrations import Migration


def test_migration_creation(tmp_path):
    """Test creating Migration instance."""
    up_file = tmp_path / "20250101000000_test_up.sql"
    down_file = tmp_path / "20250101000000_test_down.sql"

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    assert migration.version == 20250101000000
    assert migration.name == "test"
    assert migration.up_file == up_file
    assert migration.down_file == down_file


def test_migration_is_complete(tmp_path):
    """Test is_complete property."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    # Initially incomplete
    assert not migration.is_complete

    # Create up file only
    up_file.write_text("SELECT 1;")
    assert not migration.is_complete

    # Create down file
    down_file.write_text("SELECT 2;")
    assert migration.is_complete


# ==================== New Feature Tests ====================


def test_migration_dependencies_parsing(tmp_path):
    """Test dependency parsing from migration files."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    # Create files with dependency declarations
    up_file.write_text("""
-- depends_on: 20240101000000, 20240102000000
CREATE TABLE test (id INT);
""")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    deps = migration.dependencies
    assert deps == [20240101000000, 20240102000000]


def test_migration_dependencies_case_insensitive(tmp_path):
    """Test that dependency declarations are case-insensitive."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("-- DEPENDS_ON: 20240101000000\nCREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    assert migration.dependencies == [20240101000000]


def test_migration_dependencies_multiple_declarations(tmp_path):
    """Test parsing multiple dependency declarations."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("""
-- depends_on: 20240101000000
-- depends_on: 20240102000000, 20240103000000
CREATE TABLE test (id INT);
""")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    deps = migration.dependencies
    assert sorted(deps) == [20240101000000, 20240102000000, 20240103000000]


def test_migration_dependencies_no_duplicates(tmp_path):
    """Test that duplicate dependencies are deduplicated."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("""
-- depends_on: 20240101000000, 20240101000000
CREATE TABLE test (id INT);
""")
    down_file.write_text("-- depends_on: 20240101000000\nDROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    # Should deduplicate
    assert migration.dependencies == [20240101000000]


def test_migration_dependencies_empty(tmp_path):
    """Test migrations with no dependencies."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    assert migration.dependencies == []


def test_migration_dependencies_invalid_format(tmp_path):
    """Test that invalid dependency values in comma list are skipped."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    # Put valid and invalid deps on separate lines
    up_file.write_text("""
-- depends_on: 20240101000000
-- depends_on: invalid_value
-- depends_on: 20240102000000, not_a_number
CREATE TABLE test (id INT);
""")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    # Should only parse valid version numbers, skipping invalid ones
    assert sorted(migration.dependencies) == [20240101000000, 20240102000000]


def test_migration_checksum_calculation(tmp_path):
    """Test checksum calculation."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    checksum = migration.calculate_checksum()

    # Should be SHA-256 hex (64 characters)
    assert len(checksum) == 64
    assert all(c in "0123456789abcdef" for c in checksum)


def test_migration_checksum_deterministic(tmp_path):
    """Test that checksum is deterministic."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    checksum1 = migration.calculate_checksum()
    checksum2 = migration.calculate_checksum()

    assert checksum1 == checksum2


def test_migration_checksum_changes_on_modification(tmp_path):
    """Test that checksum changes when files are modified."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    checksum1 = migration.calculate_checksum()

    # Modify up file
    up_file.write_text("CREATE TABLE test (id BIGINT);")

    checksum2 = migration.calculate_checksum()

    assert checksum1 != checksum2


def test_migration_checksum_includes_both_files(tmp_path):
    """Test that checksum includes both up and down files."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    checksum1 = migration.calculate_checksum()

    # Modify down file
    down_file.write_text("DROP TABLE IF EXISTS test;")

    checksum2 = migration.calculate_checksum()

    assert checksum1 != checksum2


def test_migration_read_sql_up(tmp_path):
    """Test reading up SQL."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_content = "CREATE TABLE test (id INT);"
    down_content = "DROP TABLE test;"

    up_file.write_text(up_content)
    down_file.write_text(down_content)

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    assert migration.read_sql("up") == up_content


def test_migration_read_sql_down(tmp_path):
    """Test reading down SQL."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_content = "CREATE TABLE test (id INT);"
    down_content = "DROP TABLE test;"

    up_file.write_text(up_content)
    down_file.write_text(down_content)

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    assert migration.read_sql("down") == down_content


def test_migration_read_sql_invalid_direction(tmp_path):
    """Test that invalid direction raises ValueError."""
    up_file = tmp_path / "test_up.sql"
    down_file = tmp_path / "test_down.sql"

    up_file.write_text("CREATE TABLE test (id INT);")
    down_file.write_text("DROP TABLE test;")

    migration = Migration(
        version=20250101000000,
        name="test",
        up_file=up_file,
        down_file=down_file,
        source_dir=tmp_path,
    )

    try:
        migration.read_sql("invalid")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid direction" in str(e)
