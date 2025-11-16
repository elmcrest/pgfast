"""Database configuration."""

from dataclasses import dataclass
from urllib.parse import urlparse

from pgfast.exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Database configuration.

    Args:
        url: PostgreSQL connection URL (postgresql://...)
        min_connections: Minimum pool size (default: 5)
        max_connections: Maximum pool size (default: 20)
        timeout: Connection timeout in seconds (default: 10.0)
        command_timeout: Query timeout in seconds (default: 60.0)
        schema_dir: Directory for schema SQL files (default: "db/schema")
        migrations_dir: Directory for migrations (default: "db/migrations")
        fixtures_dir: Directory for test fixtures (default: "db/fixtures")

    Raises:
        ConfigurationError: If configuration is invalid
    """

    url: str
    min_connections: int = 5
    max_connections: int = 20
    timeout: float = 10.0
    command_timeout: float = 60.0
    schema_dir: str = "db/schema"
    migrations_dir: str = "db/migrations"
    fixtures_dir: str = "db/fixtures"

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_url()
        self._validate_pool_settings()

    def _validate_url(self) -> None:
        """Validate PostgreSQL URL format."""
        try:
            parsed = urlparse(self.url)
            if parsed.scheme not in ("postgresql", "postgres"):
                raise ConfigurationError(
                    f"Invalid database URL scheme: {parsed.scheme}. "
                    "Expected 'postgresql' or 'postgres'"
                )
        except Exception as e:
            raise ConfigurationError(f"Invalid database URL: {self.url}") from e

    def _validate_pool_settings(self) -> None:
        """Validate connection pool settings."""
        if self.min_connections <= 0:
            raise ConfigurationError(
                f"min_connections must be positive, got: {self.min_connections}"
            )

        if self.max_connections < self.min_connections:
            raise ConfigurationError(
                f"max_connections ({self.max_connections}) must be >= "
                f"min_connections ({self.min_connections})"
            )

        if self.timeout <= 0:
            raise ConfigurationError(f"timeout must be positive, got: {self.timeout}")
