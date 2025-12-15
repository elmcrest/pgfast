"""pgfast - Lightweight asyncpg integration for FastAPI.

pgfast provides schema management, migrations, and testing utilities
for FastAPI applications using PostgreSQL and asyncpg - all with raw SQL.
"""

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import (
    ConnectionError,
    MigrationError,
    PgfastError,
    SchemaError,
    TestDatabaseError,
)
from pgfast.fastapi import create_lifespan, create_rls_dependency, get_db_pool
from pgfast.schema import SchemaManager
from pgfast.testing import (
    DatabaseTestManager,
    cleanup_test_pool,
    create_test_pool_with_schema,
)

__all__ = [
    "DatabaseConfig",
    "close_pool",
    "create_pool",
    "ConnectionError",
    "MigrationError",
    "SchemaError",
    "PgfastError",
    "TestDatabaseError",
    "create_lifespan",
    "create_rls_dependency",
    "get_db_pool",
    "SchemaManager",
    "DatabaseTestManager",
    "create_test_pool_with_schema",
    "cleanup_test_pool",
]
