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
        exclude_patterns: List of directory names to exclude from auto-discovery
            (default: ["examples", "node_modules", ".venv", "venv", ".git", ".pytest_cache", "__pycache__", "dist", "build"])

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
    exclude_patterns: list[str] = [
        "examples",
        "node_modules",
        ".venv",
        "venv",
        ".git",
        ".pytest_cache",
        "__pycache__",
        "dist",
        "build",
    ]  # Directories to exclude from auto-discovery

    model_config = {"frozen": True}  # Configs shouldn't change after creation

    @classmethod
    def from_env(
        cls,
        database_url_var: str = "DATABASE_URL",
        require_url: bool = False,
    ) -> "DatabaseConfig | None":
        """Create DatabaseConfig from environment variables.

        Priority order:
        1. DATABASE_URL (or custom var name) if present
        2. Build from POSTGRES_* fragments if DATABASE_URL absent
        3. Return None if neither present (unless require_url=True, then raise)

        Supported fragment variables:
        - POSTGRES_HOST (default: localhost)
        - POSTGRES_PORT (default: 5432)
        - POSTGRES_USER (default: postgres)
        - POSTGRES_PASSWORD (optional)
        - POSTGRES_DB (required if using fragments)

        Args:
            database_url_var: Environment variable name for database URL (default: "DATABASE_URL")
            require_url: If True, raise ValueError when no config found. If False, return None.

        Returns:
            DatabaseConfig instance or None if no configuration found

        Raises:
            ValueError: If require_url=True and no configuration available,
                       or if using fragments but POSTGRES_DB is missing

        Examples:
            # From DATABASE_URL
            DATABASE_URL="postgresql://localhost/mydb"
            config = DatabaseConfig.from_env()

            # From POSTGRES_* fragments
            POSTGRES_HOST="localhost"
            POSTGRES_DB="mydb"
            config = DatabaseConfig.from_env()

            # Custom URL variable name
            MY_DB_URL="postgresql://localhost/mydb"
            config = DatabaseConfig.from_env(database_url_var="MY_DB_URL")
        """
        import os

        # Priority 1: Check for DATABASE_URL (or custom var)
        url = os.getenv(database_url_var)
        if url:
            return cls(url=url)

        # Priority 2: Try to build from POSTGRES_* fragments
        postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        postgres_user = os.getenv("POSTGRES_USER", "postgres")
        postgres_password = os.getenv("POSTGRES_PASSWORD")
        postgres_db = os.getenv("POSTGRES_DB")

        # If POSTGRES_DB is set, we have fragment-based config
        if postgres_db:
            # Build URL from fragments
            if postgres_password:
                auth = f"{postgres_user}:{postgres_password}"
            else:
                auth = postgres_user

            url = f"postgresql://{auth}@{postgres_host}:{postgres_port}/{postgres_db}"
            return cls(url=url)

        # Priority 3: No configuration found
        if require_url:
            raise ValueError(
                f"No database configuration found. Either set {database_url_var} "
                "or POSTGRES_* environment variables (POSTGRES_DB is required)."
            )

        return None

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

    def _is_excluded(self, path: Path) -> bool:
        """Check if a path should be excluded based on exclude_patterns.

        Args:
            path: Path to check

        Returns:
            True if path should be excluded, False otherwise
        """
        parts = path.parts
        for exclude in self.exclude_patterns:
            if exclude in parts:
                return True
        return False

    def discover_migrations_dirs(self) -> list[Path]:
        """Discover migration directories.

        Returns list of discovered directories, or empty list if none found.
        If migrations_dirs is explicitly set, returns those paths.
        Otherwise, performs auto-discovery using the search pattern.
        Excludes directories matching exclude_patterns.
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
        # Deduplicate discovered paths and filter excluded
        seen = set()
        result = []
        for p in matches:
            if p.is_dir() and not self._is_excluded(p):
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
        Excludes directories matching exclude_patterns.
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
        # Deduplicate discovered paths and filter excluded
        seen = set()
        result = []
        for p in matches:
            if p.is_dir() and not self._is_excluded(p):
                resolved = p.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    result.append(resolved)
        return result
