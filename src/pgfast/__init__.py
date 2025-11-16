"""pgfast - Lightweight asyncpg integration for FastAPI.

pgfast provides schema management, migrations, and testing utilities
for FastAPI applications using PostgreSQL and asyncpg - all with raw SQL.
"""

from pgfast.config import DatabaseConfig
from pgfast.connection import create_pool, close_pool
from pgfast.exceptions import (
    PgfastError,
    ConnectionError,
    SchemaError,
    ConfigurationError,
)
from pgfast.fastapi import create_lifespan, get_db_pool
from pgfast.schema import SchemaManager

__all__ = [
    "DatabaseConfig",
    "create_pool",
    "close_pool",
    "PgfastError",
    "ConnectionError",
    "SchemaError",
    "ConfigurationError",
    "create_lifespan",
    "get_db_pool",
    "SchemaManager",
]
