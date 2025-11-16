class PgfastError(Exception):
    """Base exception for all pgfast errors."""

    ...


class ConnectionError(PgfastError):
    """Raised when database connection fails."""

    ...


class SchemaError(PgfastError):
    """Raised when schema operations fail."""

    ...


class ConfigurationError(PgfastError):
    """Raised when configuration is invalid."""

    ...


class MigrationError(PgfastError):
    """Raised when migration operations fail."""

    ...


class TestDatabaseError(PgfastError):
    """Raised when test database operations fail."""

    ...
