"""Testing utilities for pgfast.

Provides utilities for creating isolated test databases, loading fixtures,
and managing test database lifecycle for fast, parallel testing.
"""

import logging
import uuid
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import asyncpg

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import TestDatabaseError
from pgfast.schema import SchemaManager

logger = logging.getLogger(__name__)

# Registry to store database names for pools
# Using pool id() as key since Pool objects don't support weak references or attribute assignment
_pool_db_names: dict[int, str] = {}


class TestDatabaseManager:
    """Manages test database lifecycle.

    This manager creates isolated PostgreSQL databases for testing,
    optionally using a template database for faster setup.

    Example:
        manager = TestDatabaseManager(config)
        pool = await manager.create_test_db()
        # Run tests...
        await manager.cleanup_test_db(pool)
    """

    def __init__(
        self,
        config: DatabaseConfig,
        template_db: Optional[str] = None,
    ):
        """Initialize test database manager.

        Args:
            config: Database configuration (should point to admin/template db)
            template_db: Name of template database to clone from (optional)
        """
        self.config = config
        self.template_db = template_db

    async def create_test_db(self, db_name: Optional[str] = None) -> asyncpg.Pool:
        """Create isolated test database.

        Args:
            db_name: Database name to create (auto-generated if None)

        Returns:
            Connection pool to the new test database

        Raises:
            TestDatabaseError: If database creation fails
        """
        # Generate unique name
        if db_name is None:
            db_name = f"pgfast_test_{uuid.uuid4().hex[:8]}"

        logger.info(f"Creating test database: {db_name}")

        # Parse DSN to connect to admin database
        try:
            parsed = urlparse(self.config.url)
            # Connect to postgres database for admin operations
            admin_dsn = parsed._replace(path="/postgres").geturl()
        except Exception as e:
            raise TestDatabaseError(f"Failed to parse database URL: {e}") from e

        # Connect to admin database
        admin_conn = None
        try:
            admin_conn = await asyncpg.connect(admin_dsn, timeout=self.config.timeout)

            # Create database from template
            if self.template_db:
                logger.info(f"Creating from template: {self.template_db}")
                # Use format() with %I for safe identifier escaping
                query = await admin_conn.fetchval(
                    "SELECT format('CREATE DATABASE %I TEMPLATE %I', $1, $2)",
                    db_name,
                    self.template_db,
                )
                await admin_conn.execute(query)
            else:
                query = await admin_conn.fetchval(
                    "SELECT format('CREATE DATABASE %I', $1)", db_name
                )
                await admin_conn.execute(query)

            # Create pool to new database
            test_dsn = parsed._replace(path=f"/{db_name}").geturl()
            test_config = DatabaseConfig(
                url=test_dsn,
                min_connections=self.config.min_connections,
                max_connections=self.config.max_connections,
                timeout=self.config.timeout,
                command_timeout=self.config.command_timeout,
            )

            pool = await create_pool(test_config)

            # Store db name in registry for cleanup (using id() as key)
            _pool_db_names[id(pool)] = db_name

            logger.info(f"Test database created successfully: {db_name}")
            return pool

        except asyncpg.PostgresError as e:
            logger.error(f"Failed to create test database: {e}")
            raise TestDatabaseError(f"Failed to create test database: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error creating test database: {e}")
            raise TestDatabaseError(
                f"Unexpected error creating test database: {e}"
            ) from e
        finally:
            if admin_conn:
                await admin_conn.close()

    async def cleanup_test_db(self, pool: asyncpg.Pool) -> None:
        """Clean up test database.

        Args:
            pool: Connection pool to the test database

        Raises:
            TestDatabaseError: If cleanup fails
        """
        # Extract database name from registry (using id() as key)
        db_name = _pool_db_names.get(id(pool))
        if not db_name:
            raise TestDatabaseError(
                "Pool not found in database registry. "
                "Was it created with TestDatabaseManager?"
            )

        logger.info(f"Cleaning up test database: {db_name}")

        # Close all connections to the database
        await close_pool(pool)

        # Parse DSN to connect to admin database
        try:
            parsed = urlparse(self.config.url)
            admin_dsn = parsed._replace(path="/postgres").geturl()
        except Exception as e:
            raise TestDatabaseError(f"Failed to parse database URL: {e}") from e

        # Connect to admin database
        admin_conn = None
        try:
            admin_conn = await asyncpg.connect(admin_dsn, timeout=self.config.timeout)

            # Terminate any remaining connections
            await admin_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = $1 AND pid <> pg_backend_pid()
                """,
                db_name,
            )

            # Drop the database
            query = await admin_conn.fetchval(
                "SELECT format('DROP DATABASE IF EXISTS %I', $1)", db_name
            )
            await admin_conn.execute(query)

            # Remove from registry
            _pool_db_names.pop(id(pool), None)

            logger.info(f"Test database cleaned up successfully: {db_name}")

        except asyncpg.PostgresError as e:
            logger.error(f"Failed to cleanup test database: {e}")
            raise TestDatabaseError(f"Failed to cleanup test database: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error cleaning up test database: {e}")
            raise TestDatabaseError(
                f"Unexpected error cleaning up test database: {e}"
            ) from e
        finally:
            if admin_conn:
                await admin_conn.close()

    async def create_template_db(
        self, template_name: str, migrations_dir: str = "db/migrations"
    ) -> str:
        """Create a template database with schema applied.

        This creates a database, applies all migrations, and marks it as a template.
        Template databases can be cloned much faster than creating and migrating
        a new database.

        Args:
            template_name: Name for the template database
            migrations_dir: Directory containing migrations

        Returns:
            Template database name

        Raises:
            TestDatabaseError: If template creation fails
        """
        logger.info(f"Creating template database: {template_name}")

        # Create the database (without template)
        pool = await self.create_test_db(db_name=template_name)

        try:
            # Apply migrations to template
            schema_manager = SchemaManager(pool, migrations_dir)
            await schema_manager.schema_up()

            await close_pool(pool)

            # Mark as template
            parsed = urlparse(self.config.url)
            admin_dsn = parsed._replace(path="/postgres").geturl()
            admin_conn = await asyncpg.connect(admin_dsn, timeout=self.config.timeout)

            try:
                await admin_conn.execute(
                    """
                    UPDATE pg_database
                    SET datistemplate = TRUE
                    WHERE datname = $1
                    """,
                    template_name,
                )
                logger.info(f"Template database created successfully: {template_name}")
                return template_name

            finally:
                await admin_conn.close()

        except Exception as e:
            # Clean up on failure
            logger.error(f"Failed to create template database: {e}")
            try:
                await close_pool(pool)
                await self.cleanup_test_db(pool)
            except Exception:
                pass  # Best effort cleanup
            raise TestDatabaseError(f"Failed to create template database: {e}") from e

    async def destroy_template_db(self, template_name: str) -> None:
        """Destroy a template database.

        Args:
            template_name: Name of the template database to destroy

        Raises:
            TestDatabaseError: If template destruction fails
        """
        logger.info(f"Destroying template database: {template_name}")

        parsed = urlparse(self.config.url)
        admin_dsn = parsed._replace(path="/postgres").geturl()
        admin_conn = None

        try:
            admin_conn = await asyncpg.connect(admin_dsn, timeout=self.config.timeout)

            # Mark as non-template first
            await admin_conn.execute(
                """
                UPDATE pg_database
                SET datistemplate = FALSE
                WHERE datname = $1
                """,
                template_name,
            )

            # Terminate connections
            await admin_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = $1 AND pid <> pg_backend_pid()
                """,
                template_name,
            )

            # Drop the database
            query = await admin_conn.fetchval(
                "SELECT format('DROP DATABASE IF EXISTS %I', $1)", template_name
            )
            await admin_conn.execute(query)

            logger.info(f"Template database destroyed successfully: {template_name}")

        except asyncpg.PostgresError as e:
            logger.error(f"Failed to destroy template database: {e}")
            raise TestDatabaseError(f"Failed to destroy template database: {e}") from e
        finally:
            if admin_conn:
                await admin_conn.close()

    async def load_fixtures(
        self, pool: asyncpg.Pool, fixtures: list[Path | str]
    ) -> None:
        """Load SQL fixture files into the database.

        Args:
            pool: Connection pool to load fixtures into
            fixtures: List of fixture file paths

        Raises:
            TestDatabaseError: If fixture loading fails
        """
        logger.info(f"Loading {len(fixtures)} fixture(s)")

        async with pool.acquire() as conn:
            for fixture in fixtures:
                fixture_path = Path(fixture)

                if not fixture_path.exists():
                    raise TestDatabaseError(
                        f"Fixture file does not exist: {fixture_path}"
                    )

                logger.debug(f"Loading fixture: {fixture_path}")

                try:
                    sql = fixture_path.read_text()
                    await conn.execute(sql)
                except asyncpg.PostgresError as e:
                    raise TestDatabaseError(
                        f"Failed to load fixture {fixture_path}: {e}"
                    ) from e
                except Exception as e:
                    raise TestDatabaseError(
                        f"Unexpected error loading fixture {fixture_path}: {e}"
                    ) from e

        logger.info("Fixtures loaded successfully")


async def create_test_pool_with_schema(
    config: DatabaseConfig,
    migrations_dir: str = "db/migrations",
) -> asyncpg.Pool:
    """Create test database with schema applied.

    This is a convenience function that:
    1. Creates an isolated test database
    2. Applies all migrations
    3. Returns the connection pool

    Example:
        pool = await create_test_pool_with_schema(config)
        # Run tests...
        await cleanup_test_pool(pool)

    Args:
        config: Database configuration
        migrations_dir: Directory containing migrations

    Returns:
        Connection pool to test database with schema applied

    Raises:
        TestDatabaseError: If creation fails
    """
    manager = TestDatabaseManager(config)
    pool = await manager.create_test_db()

    try:
        # Apply migrations
        schema_manager = SchemaManager(pool, migrations_dir)
        await schema_manager.schema_up()
        return pool
    except Exception as _e:
        # Clean up on failure
        try:
            await manager.cleanup_test_db(pool)
        except Exception:
            pass  # Best effort cleanup
        raise


async def cleanup_test_pool(pool: asyncpg.Pool, config: DatabaseConfig) -> None:
    """Clean up test database pool.

    Convenience function for cleanup_test_db.

    Args:
        pool: Connection pool to clean up
        config: Database configuration for admin connection

    Raises:
        TestDatabaseError: If cleanup fails
    """
    manager = TestDatabaseManager(config)
    await manager.cleanup_test_db(pool)
