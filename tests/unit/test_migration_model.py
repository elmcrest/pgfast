"""Unit tests for Migration model."""

from pgfast.schema import Migration


def test_migration_creation(tmp_path):
    """Test creating Migration instance."""
    up_file = tmp_path / "20250101000000_test_up.sql"
    down_file = tmp_path / "20250101000000_test_down.sql"

    migration = Migration(
        version=20250101000000, name="test", up_file=up_file, down_file=down_file
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
        version=20250101000000, name="test", up_file=up_file, down_file=down_file
    )

    # Initially incomplete
    assert not migration.is_complete

    # Create up file only
    up_file.write_text("SELECT 1;")
    assert not migration.is_complete

    # Create down file
    down_file.write_text("SELECT 2;")
    assert migration.is_complete
