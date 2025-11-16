import logging
from typing import Optional

import asyncpg

from pgfast.config import DatabaseConfig
from pgfast.exceptions import ConnectionError

logger = logging.getLogger(__name__)


async def create_pool(config: DatabaseConfig) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool.

    Args:
        config: Database configuration

    Returns:
        asyncpg.Pool: Configured connection pool

    Raises:
        ConnectionError: If pool creation fails
    """
    logger.info(
        f"Creating connection pool (min={config.min_connections}, "
        f"max={config.max_connections})"
    )

    try:
        pool = await asyncpg.create_pool(
            dsn=config.url,
            min_size=config.min_connections,
            max_size=config.max_connections,
            timeout=config.timeout,
            command_timeout=config.command_timeout,
        )

        if pool is None:
            raise ConnectionError("Pool creation returned None")

        # Verify pool by acquiring a connection
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        logger.info("Connection pool created successfully")
        return pool

    except asyncpg.PostgresError as e:
        logger.error(f"Failed to create connection pool: {e}")
        raise ConnectionError(f"Failed to connect to database: {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error creating pool: {e}")
        raise ConnectionError(f"Unexpected error: {e}") from e


async def close_pool(pool: Optional[asyncpg.Pool]) -> None:
    """Close the connection pool gracefully.

    Args:
        pool: Connection pool to close (can be None)
    """
    if pool is None:
        return

    logger.info("Closing connection pool")

    try:
        await pool.close()
        logger.info("Connection pool closed successfully")
    except Exception as e:
        logger.error(f"Error closing pool: {e}")
        # Don't raise - best effort cleanup
