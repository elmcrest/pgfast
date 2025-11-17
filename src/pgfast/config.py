"""Database configuration."""

from pathlib import Path
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
        migrations_dirs: Optional list of migration directories. If None, auto-discover
        fixtures_dirs: Optional list of fixture directories. If None, auto-discover
        migrations_search_pattern: Glob pattern for discovering migrations (default: "**/migrations")
        fixtures_search_pattern: Glob pattern for discovering fixtures (default: "**/fixtures")
        search_base_path: Base path for auto-discovery (default: current working directory)

    Raises:
        ValidationError: If configuration is invalid
    """

    url: str
    min_connections: int = Field(default=5, gt=0)
    max_connections: int = Field(default=20, gt=0)
    timeout: float = Field(default=10.0, gt=0)
    command_timeout: float = Field(default=60.0, gt=0)

    # Optional explicit directory configuration
    # If None, auto-discovery is used
    migrations_dirs: list[str] | None = None
    fixtures_dirs: list[str] | None = None

    # Search configuration for auto-discovery
    migrations_search_pattern: str = "**/migrations"
    fixtures_search_pattern: str = "**/fixtures"
    search_base_path: Path | None = None  # None = cwd()

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

    def discover_migrations_dirs(self) -> list[Path]:
        """Discover migration directories.

        Returns list of discovered directories, or empty list if none found.
        If migrations_dirs is explicitly set, returns those paths.
        Otherwise, performs auto-discovery using the search pattern.
        """
        if self.migrations_dirs is not None:
            # Explicit configuration - deduplicate paths
            seen = set()
            result = []
            for d in self.migrations_dirs:
                p = Path(d).resolve()  # Resolve to absolute path for deduplication
                if p not in seen:
                    seen.add(p)
                    result.append(p)
            return result

        # Auto-discover
        base = self.search_base_path or Path.cwd()
        matches = sorted(base.glob(self.migrations_search_pattern))
        # Deduplicate discovered paths
        seen = set()
        result = []
        for p in matches:
            if p.is_dir():
                resolved = p.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    result.append(resolved)
        return result

    def discover_fixtures_dirs(self) -> list[Path]:
        """Discover fixture directories.

        Returns list of discovered directories, or empty list if none found.
        If fixtures_dirs is explicitly set, returns those paths.
        Otherwise, performs auto-discovery using the search pattern.
        """
        if self.fixtures_dirs is not None:
            # Explicit configuration - deduplicate paths
            seen = set()
            result = []
            for d in self.fixtures_dirs:
                p = Path(d).resolve()  # Resolve to absolute path for deduplication
                if p not in seen:
                    seen.add(p)
                    result.append(p)
            return result

        # Auto-discover
        base = self.search_base_path or Path.cwd()
        matches = sorted(base.glob(self.fixtures_search_pattern))
        # Deduplicate discovered paths
        seen = set()
        result = []
        for p in matches:
            if p.is_dir():
                resolved = p.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    result.append(resolved)
        return result
