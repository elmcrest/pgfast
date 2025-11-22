"""Testing utilities for pgfast.

Provides utilities for creating isolated test databases, loading fixtures,
and managing test database lifecycle for fast, parallel testing.
"""

import asyncio
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


class DatabaseTestManager:
    """Manages test database lifecycle.

    This manager creates isolated PostgreSQL databases for testing,
    optionally using a template database for faster setup.

    Example:
        manager = DatabaseTestManager(config)
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

            # Try to create database from template (fast path with retry)
            if self.template_db:
                logger.info(f"Creating from template: {self.template_db}")

                # Retry configuration for template cloning with exponential backoff
                # Most locks are very brief (< 50ms), so we use aggressive retries
                max_retries = 10
                base_delay = 0.01  # Start with 10ms
                for attempt in range(max_retries):
                    try:
                        # Use format() with %I for safe identifier escaping
                        query = await admin_conn.fetchval(
                            "SELECT format('CREATE DATABASE %I TEMPLATE %I', $1::text, $2::text)",
                            db_name,
                            self.template_db,
                        )
                        await admin_conn.execute(query)
                        # Success! Break out of retry loop
                        break
                    except asyncpg.PostgresError as e:
                        # Check if template is locked (error code 55006 or message contains "being accessed")
                        if "being accessed by other users" in str(e).lower():
                            if attempt < max_retries - 1:
                                # Exponential backoff: 10ms, 20ms, 40ms, 80ms, 160ms...
                                # Capped at 100ms max delay
                                delay = min(base_delay * (2**attempt), 0.1)
                                logger.debug(
                                    f"Template database locked, retrying in {delay:.3f}s (attempt {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                # All retries exhausted, fall back to slow path
                                logger.warning(
                                    f"Template database locked after {max_retries} attempts, falling back to no-template creation for {db_name}"
                                )
                                # Fallback: create without template and apply migrations
                                query = await admin_conn.fetchval(
                                    "SELECT format('CREATE DATABASE %I', $1::text)",
                                    db_name,
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

                                # Apply migrations since we couldn't use template
                                schema_manager = SchemaManager(pool, self.config)
                                await schema_manager.schema_up()

                                # Store db name in registry for cleanup (using id() as key)
                                _pool_db_names[id(pool)] = db_name

                                logger.info(
                                    f"Test database created successfully (no-template fallback): {db_name}"
                                )
                                return pool
                        else:
                            # Re-raise if it's a different error
                            raise
            else:
                # No template specified, create empty database
                query = await admin_conn.fetchval(
                    "SELECT format('CREATE DATABASE %I', $1::text)", db_name
                )
                await admin_conn.execute(query)

            # Create pool to new database (template creation succeeded)
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
                "Was it created with DatabaseTestManager?"
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
                "SELECT format('DROP DATABASE IF EXISTS %I', $1::text)", db_name
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

    async def create_template_db(self, template_name: str) -> str:
        """Create a template database with schema applied.

        This creates a database, applies all migrations from all configured
        migration directories, and marks it as a template. Template databases
        can be cloned much faster than creating and migrating a new database.

        Args:
            template_name: Name for the template database

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
            schema_manager = SchemaManager(pool, self.config)
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
                "SELECT format('DROP DATABASE IF EXISTS %I', $1::text)", template_name
            )
            await admin_conn.execute(query)

            logger.info(f"Template database destroyed successfully: {template_name}")

        except asyncpg.PostgresError as e:
            logger.error(f"Failed to destroy template database: {e}")
            raise TestDatabaseError(f"Failed to destroy template database: {e}") from e
        finally:
            if admin_conn:
                await admin_conn.close()

    def _sort_fixtures_by_dependencies(self, fixtures: list) -> list[Path]:
        """Sort fixtures by migration dependency order.

        Args:
            fixtures: List of Fixture objects

        Returns:
            List of fixture paths sorted by migration dependency order
        """
        from pgfast.fixtures import Fixture

        # Get migration dependency graph
        schema_manager = SchemaManager(
            pool=None,  # type: ignore - not needed for discovery
            config=self.config,
        )
        all_migrations = schema_manager._discover_migrations()
        migration_map = {m.version: m for m in all_migrations}

        # Sort fixtures by their corresponding migration dependencies
        # Build a version -> fixture mapping
        fixture_map: dict[int, Fixture] = {}
        for fixture in fixtures:
            fixture_map[fixture.version] = fixture

        # Get only migrations that have corresponding fixtures
        migrations_with_fixtures = [
            migration_map[v] for v in fixture_map.keys() if v in migration_map
        ]

        if not migrations_with_fixtures:
            # No matching migrations found, return fixtures sorted by version
            return [f.path for f in sorted(fixtures, key=lambda f: f.version)]

        # Apply topological sort to migrations
        sorted_migrations = schema_manager._topological_sort(migrations_with_fixtures)

        # Return fixture paths in migration dependency order
        result = []
        for migration in sorted_migrations:
            if migration.version in fixture_map:
                result.append(fixture_map[migration.version].path)

        return result

    def discover_fixtures(self) -> list[Path]:
        """Discover all fixture files across multiple directories.

        Returns:
            List of fixture paths sorted by migration dependency order
        """
        from pgfast.fixtures import Fixture

        all_fixtures = []

        fixtures_dirs = self.config.discover_fixtures_dirs()

        for fixtures_dir in fixtures_dirs:
            if not fixtures_dir.exists():
                continue

            # Only include files matching the fixture naming convention
            # Recursively search for fixtures in subdirectories
            for sql_file in fixtures_dir.glob("**/*.sql"):
                fixture = Fixture.from_path(sql_file)
                if fixture:
                    all_fixtures.append(fixture)

        if not all_fixtures:
            return []

        # Sort fixtures by migration dependency order
        return self._sort_fixtures_by_dependencies(all_fixtures)

    async def load_fixtures(
        self, pool: asyncpg.Pool, fixtures: list[Path | str] | None = None
    ) -> None:
        """Load SQL fixture files into the database.

        Fixtures are loaded in migration dependency order to ensure proper
        referential integrity.

        Args:
            pool: Connection pool to load fixtures into
            fixtures: List of fixture file paths. If None, auto-discover from all
                     configured fixture directories.

        Raises:
            TestDatabaseError: If fixture loading fails
        """
        from pgfast.fixtures import Fixture

        # Get fixture paths sorted by dependencies
        if fixtures is None:
            # Auto-discover
            fixture_paths = self.discover_fixtures()
        else:
            # Parse explicit paths to Fixture objects for sorting
            fixture_objs = []
            for fixture_path in fixtures:
                path = Path(fixture_path)
                fixture = Fixture.from_path(path)
                if fixture:
                    fixture_objs.append(fixture)
                else:
                    # Not a versioned fixture, just add path as-is
                    # (backward compatibility for non-versioned fixtures)
                    logger.warning(
                        f"Fixture {path} doesn't follow naming convention, "
                        "loading without dependency ordering"
                    )

            # Sort by dependencies if we have versioned fixtures
            if fixture_objs:
                fixture_paths = self._sort_fixtures_by_dependencies(fixture_objs)
                # Add any non-versioned fixtures at the end
                non_versioned = [
                    Path(f)
                    for f in fixtures
                    if not Fixture.from_path(Path(f)) and Path(f).suffix == ".sql"
                ]
                fixture_paths.extend(non_versioned)
            else:
                # All non-versioned, use as-is
                fixture_paths = [Path(f) for f in fixtures]

        if not fixture_paths:
            logger.info("No fixtures to load")
            return

        logger.info(f"Loading {len(fixture_paths)} fixture(s)")

        async with pool.acquire() as conn:
            for fixture_path in fixture_paths:
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


async def create_test_pool_with_schema(config: DatabaseConfig) -> asyncpg.Pool:
    """Create test database with schema applied.

    This is a convenience function that:
    1. Creates an isolated test database
    2. Applies all migrations from all configured directories
    3. Returns the connection pool

    Example:
        pool = await create_test_pool_with_schema(config)
        # Run tests...
        await cleanup_test_pool(pool)

    Args:
        config: Database configuration

    Returns:
        Connection pool to test database with schema applied

    Raises:
        TestDatabaseError: If creation fails
    """
    manager = DatabaseTestManager(config)
    pool = await manager.create_test_db()

    try:
        # Apply migrations
        schema_manager = SchemaManager(pool, config)
        await schema_manager.schema_up()
        return pool
    except Exception:
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
    manager = DatabaseTestManager(config)
    await manager.cleanup_test_db(pool)
