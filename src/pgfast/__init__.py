"""pgfast - Lightweight asyncpg integration for FastAPI.

pgfast provides schema management, migrations, and testing utilities
for FastAPI applications using PostgreSQL and asyncpg - all with raw SQL.
"""

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import (
    ConfigurationError,
    ConnectionError,
    MigrationError,
    PgfastError,
    SchemaError,
)
from pgfast.fastapi import create_lifespan, get_db_pool
from pgfast.schema import SchemaManager

__all__ = [
    "DatabaseConfig",
    "close_pool",
    "create_pool",
    "ConfigurationError",
    "ConnectionError",
    "MigrationError",
    "SchemaError",
    "PgfastError",
    "create_lifespan",
    "get_db_pool",
    "SchemaManager",
]
