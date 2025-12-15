"""FastAPI integration for pgfast."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Awaitable, Callable, Optional

import asyncpg
from fastapi import FastAPI, Request

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool

logger = logging.getLogger(__name__)


def create_lifespan(config: DatabaseConfig):
    """Create a lifespan context manager for database pool management.

    This function returns an async context manager that handles the database
    connection pool lifecycle for FastAPI applications.

    Example:
        from fastapi import FastAPI
        from pgfast import DatabaseConfig, create_lifespan

        config = DatabaseConfig(url="postgresql://localhost/mydb")

        app = FastAPI(lifespan=create_lifespan(config))

    For composing multiple lifespan handlers:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def combined_lifespan(app: FastAPI):
            # Your other startup code
            async with create_lifespan(config)(app):
                # Additional startup
                yield
                # Additional cleanup
            # Your other shutdown code

        app = FastAPI(lifespan=combined_lifespan)

    Args:
        config: Database configuration

    Returns:
        An async context manager function for FastAPI lifespan
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Manage database connection pool lifecycle."""
        # Startup: Create connection pool
        logger.info("Initializing database connection pool")
        pool = await create_pool(config)
        app.state.db_pool = pool
        logger.info("Database connection pool initialized")

        yield

        # Shutdown: Close connection pool
        logger.info("Shutting down database connection pool")
        pool_instance: Optional[asyncpg.Pool] = getattr(app.state, "db_pool", None)
        await close_pool(pool_instance)
        logger.info("Database connection pool shut down")

    return lifespan


async def get_db_pool(request: Request) -> asyncpg.Pool:
    """Dependency to get database pool from request.

    Usage:
        from fastapi import Depends
        from pgfast import get_db_pool

        @app.get("/users")
        async def get_users(pool: asyncpg.Pool = Depends(get_db_pool)):
            async with pool.acquire() as conn:
                return await conn.fetch("SELECT * FROM users")

    Args:
        request: FastAPI request object

    Returns:
        asyncpg.Pool: Database connection pool
    """
    return request.app.state.db_pool


def create_rls_dependency(
    get_settings: Callable[[Request], Awaitable[dict[str, str]]],
) -> Callable[[Request], AsyncGenerator[asyncpg.Connection, None]]:
    """Create a FastAPI dependency for RLS-aware database connections.

    This factory creates a dependency that acquires a connection, sets
    session variables using SET LOCAL within a transaction, and yields
    the connection. Variables are automatically cleared when the
    transaction ends.

    Using SET LOCAL ensures compatibility with PgBouncer in transaction
    pooling mode - settings don't leak between clients.

    Example:
        from fastapi import Depends, Request
        from pgfast import create_rls_dependency

        async def get_tenant_settings(request: Request) -> dict[str, str]:
            # Extract tenant from JWT, header, etc.
            tenant_id = request.headers.get("X-Tenant-ID", "")
            return {"app.tenant_id": tenant_id}

        # Create the dependency
        get_rls_connection = create_rls_dependency(get_tenant_settings)

        @app.get("/items")
        async def list_items(
            conn: asyncpg.Connection = Depends(get_rls_connection)
        ):
            # RLS policies using current_setting('app.tenant_id') work here
            return await conn.fetch("SELECT * FROM items")

    For multiple settings (e.g., tenant + user):
        async def get_rls_settings(request: Request) -> dict[str, str]:
            return {
                "app.tenant_id": request.state.tenant_id,
                "app.user_id": request.state.user_id,
                "app.role": request.state.role,
            }

    Note:
        All queries within the dependency execute inside a transaction.
        This is required for SET LOCAL to work correctly. For read-only
        queries this has no practical impact (PostgreSQL uses implicit
        transactions anyway). For write operations, be aware you're
        already in a transaction context.

    Args:
        get_settings: Async function that extracts RLS settings from the
            request. Returns a dict mapping setting names to values.
            Setting names should include the namespace (e.g., "app.tenant_id").

    Returns:
        A FastAPI dependency that yields an asyncpg.Connection with
        RLS session variables set.
    """

    async def dependency(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
        pool: asyncpg.Pool = request.app.state.db_pool
        settings = await get_settings(request)

        async with pool.acquire() as conn:
            async with conn.transaction():
                for key, value in settings.items():
                    # Use set_config() for parameterized, injection-safe setting
                    # Third parameter `true` = LOCAL (transaction-scoped)
                    await conn.execute("SELECT set_config($1, $2, true)", key, value)
                yield conn

    return dependency
