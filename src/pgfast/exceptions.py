class PgfastError(Exception):
    """Base exception for all pgfast errors."""

    ...


class ConnectionError(PgfastError):
    """Raised when database connection fails."""

    ...


class SchemaError(PgfastError):
    """Raised when schema operations fail."""

    ...


class MigrationError(PgfastError):
    """Raised when migration operations fail."""

    def __init__(self, message: str, applied_migrations: list[int] | None = None):
        """Initialize MigrationError.

        Args:
            message: Error message
            applied_migrations: List of migration versions that were successfully
                applied before the error occurred (if applicable)
        """
        super().__init__(message)
        self.applied_migrations = applied_migrations or []


class TestDatabaseError(PgfastError):
    """Raised when test database operations fail."""

    ...


class DependencyError(PgfastError):
    """Raised when migration dependencies are invalid or unmet."""

    ...


class ChecksumError(PgfastError):
    """Raised when migration file checksums don't match."""

    ...
