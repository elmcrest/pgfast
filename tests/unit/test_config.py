import pytest
from pydantic import ValidationError

from pgfast.config import DatabaseConfig


def test_config_with_valid_url():
    """Should create config with valid PostgreSQL URL."""
    config = DatabaseConfig(url="postgresql://user:pass@localhost:5432/dbname")
    assert config.url == "postgresql://user:pass@localhost:5432/dbname"
    assert config.min_connections == 5  # default
    assert config.max_connections == 20  # default


def test_config_with_shortened_valid_url():
    """Should create config with valid PostgreSQL URL."""
    config = DatabaseConfig(url="dbname")
    assert config.url == "postgresql://postgres@localhost:5432/dbname"
    config = DatabaseConfig(url="localhost/dbname")
    assert config.url == "postgresql://postgres@localhost:5432/dbname"
    config = DatabaseConfig(url="localhost:5432/dbname")
    assert config.url == "postgresql://postgres@localhost:5432/dbname"
    config = DatabaseConfig(url="postgres@localhost:5432/dbname")
    assert config.url == "postgresql://postgres@localhost:5432/dbname"
    config = DatabaseConfig(url="postgres:postgres@localhost:5432/dbname")
    assert config.url == "postgresql://postgres:postgres@localhost:5432/dbname"


def test_config_with_custom_pool_settings():
    """Should allow custom pool configuration."""
    config = DatabaseConfig(
        url="postgresql://localhost/test",
        min_connections=2,
        max_connections=10,
        timeout=5.0,
    )
    assert config.min_connections == 2
    assert config.max_connections == 10
    assert config.timeout == 5.0


def test_config_rejects_invalid_pool_size():
    """Should reject invalid pool sizes."""
    with pytest.raises(ValidationError):
        DatabaseConfig(url="postgresql://localhost/test", min_connections=0)

    with pytest.raises(ValidationError):
        DatabaseConfig(
            url="postgresql://localhost/test", min_connections=10, max_connections=5
        )


def test_config_with_explicit_dirs():
    """Should accept explicit migration and fixture directories."""
    config = DatabaseConfig(
        url="postgresql://localhost/test",
        migrations_dirs=["app/users/migrations", "app/posts/migrations"],
        fixtures_dirs=["app/users/fixtures", "app/posts/fixtures"],
    )
    assert config.migrations_dirs == ["app/users/migrations", "app/posts/migrations"]
    assert config.fixtures_dirs == ["app/users/fixtures", "app/posts/fixtures"]


def test_config_auto_discovery_defaults():
    """Should have None for dirs by default to enable auto-discovery."""
    config = DatabaseConfig(url="postgresql://localhost/test")
    assert config.migrations_dirs is None
    assert config.fixtures_dirs is None
