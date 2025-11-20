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


def test_from_env_with_database_url(monkeypatch):
    """Should create config from DATABASE_URL environment variable."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/mydb")
    config = DatabaseConfig.from_env()
    assert config is not None
    assert config.url == "postgresql://user:pass@localhost:5432/mydb"


def test_from_env_with_postgres_fragments(monkeypatch):
    """Should create config from POSTGRES_* fragment variables."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_HOST", "dbhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "myuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "mypass")
    monkeypatch.setenv("POSTGRES_DB", "mydb")

    config = DatabaseConfig.from_env()
    assert config is not None
    assert config.url == "postgresql://myuser:mypass@dbhost:5433/mydb"


def test_from_env_fragments_with_defaults(monkeypatch):
    """Should use defaults for missing fragment variables."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    # Not setting POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER

    config = DatabaseConfig.from_env()
    assert config is not None
    assert config.url == "postgresql://postgres@localhost:5432/testdb"


def test_from_env_fragments_without_password(monkeypatch):
    """Should build URL without password if POSTGRES_PASSWORD not set."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    # Not setting POSTGRES_PASSWORD

    config = DatabaseConfig.from_env()
    assert config is not None
    assert config.url == "postgresql://testuser@localhost:5432/testdb"


def test_from_env_database_url_priority(monkeypatch):
    """DATABASE_URL should take priority over POSTGRES_* fragments."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://from-url@host1/db1")
    monkeypatch.setenv("POSTGRES_HOST", "host2")
    monkeypatch.setenv("POSTGRES_DB", "db2")

    config = DatabaseConfig.from_env()
    assert config is not None
    # Should use DATABASE_URL, not fragments
    assert config.url == "postgresql://from-url@host1:5432/db1"


def test_from_env_no_config_returns_none(monkeypatch):
    """Should return None when no configuration is available."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)

    config = DatabaseConfig.from_env()
    assert config is None


def test_from_env_no_config_with_require_raises(monkeypatch):
    """Should raise ValueError when require_url=True and no config available."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)

    with pytest.raises(ValueError) as exc_info:
        DatabaseConfig.from_env(require_url=True)

    assert "No database configuration found" in str(exc_info.value)
    assert "DATABASE_URL" in str(exc_info.value)
    assert "POSTGRES_*" in str(exc_info.value)


def test_from_env_custom_url_var(monkeypatch):
    """Should support custom database URL variable name."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("MY_DB_URL", "postgresql://custom@host/customdb")

    config = DatabaseConfig.from_env(database_url_var="MY_DB_URL")
    assert config is not None
    assert config.url == "postgresql://custom@host:5432/customdb"
