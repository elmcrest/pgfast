"""Schema management for pgfast."""

import logging
from pathlib import Path

import asyncpg

from pgfast.exceptions import SchemaError

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schema operations.

    Args:
        pool: asyncpg connection pool
        schema_dir: Directory containing schema SQL files
    """

    def __init__(self, pool: asyncpg.Pool, schema_dir: str = "db/schema"):
        self.pool = pool
        self.schema_dir = Path(schema_dir)

    async def apply_schema(self) -> None:
        """Apply all schema files from schema directory.

        Loads and executes all .sql files in the schema directory
        in alphabetical order. All operations are performed in a
        single transaction for atomicity.

        Raises:
            SchemaError: If schema directory doesn't exist or SQL execution fails
        """
        if not self.schema_dir.exists():
            raise SchemaError(f"Schema directory not found: {self.schema_dir}")

        # Get all SQL files sorted by name
        sql_files = sorted(self.schema_dir.glob("*.sql"))

        if not sql_files:
            logger.warning(f"No schema files found in {self.schema_dir}")
            return

        logger.info(f"Applying {len(sql_files)} schema file(s)")

        async with self.pool.acquire() as conn:
            # Use transaction for atomicity
            async with conn.transaction():
                for sql_file in sql_files:
                    logger.info(f"Executing schema file: {sql_file.name}")

                    try:
                        sql_content = sql_file.read_text()
                        await conn.execute(sql_content)
                        logger.info(f"Successfully executed: {sql_file.name}")

                    except asyncpg.PostgresError as e:
                        logger.error(f"Failed to execute {sql_file.name}: {e}")
                        raise SchemaError(
                            f"Failed to apply schema file {sql_file.name}: {e}"
                        ) from e

        logger.info("Schema applied successfully")
