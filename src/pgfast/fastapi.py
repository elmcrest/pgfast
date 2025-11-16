"""FastAPI integration for pgfast."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import asyncpg
from fastapi import FastAPI, Request

from pgfast.config import DatabaseConfig
from pgfast.connection import create_pool, close_pool

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
