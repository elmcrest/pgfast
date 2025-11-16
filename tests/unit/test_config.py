import pytest
from pgfast.config import DatabaseConfig
from pgfast.exceptions import ConfigurationError


def test_config_with_valid_url():
    """Should create config with valid PostgreSQL URL."""
    config = DatabaseConfig(url="postgresql://user:pass@localhost:5432/dbname")
    assert config.url == "postgresql://user:pass@localhost:5432/dbname"
    assert config.min_connections == 5  # default
    assert config.max_connections == 20  # default


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
    with pytest.raises(ConfigurationError, match="min_connections must be positive"):
        DatabaseConfig(url="postgresql://localhost/test", min_connections=0)

    with pytest.raises(
        ConfigurationError,
        match="max_connections \(5\) must be >= min_connections \(10\)",
    ):
        DatabaseConfig(
            url="postgresql://localhost/test", min_connections=10, max_connections=5
        )


def test_config_validates_url_format():
    """Should validate PostgreSQL URL format."""
    with pytest.raises(ConfigurationError, match="Invalid database URL"):
        DatabaseConfig(url="not-a-valid-url")

    with pytest.raises(ConfigurationError, match="Invalid database URL"):
        DatabaseConfig(url="mysql://localhost/test")  # Wrong database type


def test_config_with_paths():
    """Should accept custom migration paths."""
    config = DatabaseConfig(
        url="postgresql://localhost/test",
        migrations_dir="custom/migrations",
    )
    assert config.migrations_dir == "custom/migrations"
