"""Database configuration."""

from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator


class DatabaseConfig(BaseModel):
    """Database configuration.

    Args:
        url: PostgreSQL connection URL (postgresql://...)
        min_connections: Minimum pool size (default: 5)
        max_connections: Maximum pool size (default: 20)
        timeout: Connection timeout in seconds (default: 10.0)
        command_timeout: Query timeout in seconds (default: 60.0)
        migrations_dir: Directory for migrations (default: "db/migrations")
        fixtures_dir: Directory for test fixtures (default: "db/fixtures")

    Raises:
        ValidationError: If configuration is invalid
    """

    url: str
    min_connections: int = Field(default=5, gt=0)
    max_connections: int = Field(default=20, gt=0)
    timeout: float = Field(default=10.0, gt=0)
    command_timeout: float = Field(default=60.0, gt=0)
    migrations_dir: str = "db/migrations"
    fixtures_dir: str = "db/fixtures"

    model_config = {"frozen": True}  # Configs shouldn't change after creation

    @field_validator("max_connections")
    @classmethod
    def validate_max_connections(cls, v: int, info) -> int:
        """Validate max_connections is >= min_connections."""
        # Note: min_connections is validated first due to field order
        min_conn = info.data.get("min_connections", 5)
        if v < min_conn:
            raise ValueError(
                f"max_connections ({v}) must be >= min_connections ({min_conn})"
            )
        return v

    @model_validator(mode="after")
    def validate_and_normalize_url(self) -> "DatabaseConfig":
        """Validate and normalize PostgreSQL URL with defaults.

        Applies PostgreSQL default values for missing components:
        - Host: localhost
        - Port: 5432
        - User: postgres
        - Database: same as username (or database name if provided alone)

        Examples:
            "dbname" → "postgresql://postgres@localhost:5432/dbname"
            "localhost/dbname" → "postgresql://postgres@localhost:5432/dbname"
            "postgres@localhost:5432/dbname" → "postgresql://postgres@localhost:5432/dbname"
        """
        try:
            url = self.url

            # Handle case where scheme is missing
            if not url.startswith(("postgresql://", "postgres://")):
                # If contains "/", it has host info before the "/"
                if "/" in url:
                    url = f"postgresql://{url}"
                else:
                    # Just database name
                    url = f"postgresql:///{url}"

            parsed = urlparse(url)

            # Validate scheme
            if parsed.scheme not in ("postgresql", "postgres"):
                raise ValueError(
                    f"Invalid database URL scheme: {parsed.scheme}. "
                    "Expected 'postgresql' or 'postgres'"
                )

            # Apply defaults (PostgreSQL-style)
            scheme = "postgresql"  # Normalize to postgresql
            username = parsed.username or "postgres"
            password = parsed.password
            hostname = parsed.hostname or "localhost"
            port = parsed.port or 5432

            # Database defaults to username if not specified, or if path is just "/"
            database = (
                parsed.path.lstrip("/")
                if parsed.path and parsed.path != "/"
                else username
            )

            # Reconstruct URL with all components
            if password:
                auth = f"{username}:{password}"
            else:
                auth = username

            normalized_url = f"{scheme}://{auth}@{hostname}:{port}/{database}"

            # Use object.__setattr__ since model is frozen
            object.__setattr__(self, "url", normalized_url)

            return self

        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid database URL: {self.url}") from e
