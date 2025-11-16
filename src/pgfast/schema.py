"""Schema management for pgfast."""

import logging
from datetime import datetime
from pathlib import Path

import asyncpg

from pgfast.exceptions import MigrationError, SchemaError
from pgfast.migrations import Migration

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database migrations.

    Args:
        pool: asyncpg connection pool
        migrations_dir: Directory containing migration files
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        migrations_dir: str = "db/migrations",
    ):
        self.pool = pool
        self.migrations_dir = Path(migrations_dir)

    def _discover_migrations(self) -> list[Migration]:
        """Discover all migrations in migrations directory.

        Returns:
            List of Migration objects sorted by version

        Raises:
            SchemaError: If migrations directory doesn't exist
            MigrationError: If migration files are malformed
        """
        if not self.migrations_dir.exists():
            raise SchemaError(f"Migrations directory not found: {self.migrations_dir}")

        # Find all _up.sql files
        up_files = list(self.migrations_dir.glob("*_up.sql"))

        migrations = []
        for up_file in up_files:
            # Parse filename: {version}_{name}_up.sql
            parts = up_file.stem.split("_")
            if len(parts) < 3:
                raise MigrationError(f"Invalid migration filename: {up_file.name}")

            version_str = parts[0]
            name = "_".join(parts[1:-1])  # Everything between version and "up"

            try:
                version = int(version_str)
            except ValueError:
                raise MigrationError(f"Invalid version in filename: {up_file.name}")

            # Find corresponding down file
            down_file = up_file.parent / f"{version_str}_{name}_down.sql"

            migrations.append(
                Migration(
                    version=version, name=name, up_file=up_file, down_file=down_file
                )
            )

        # Sort by version
        return sorted(migrations, key=lambda m: m.version)

    async def _ensure_migrations_table(self) -> None:
        """Create migrations tracking table if it doesn't exist."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS _pgfast_migrations (
                    version BIGINT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMP DEFAULT NOW()
                )
            """)

    async def get_current_version(self) -> int:
        """Get the current schema version (latest applied migration).

        Returns:
            Version number of the latest applied migration, or 0 if none
        """
        await self._ensure_migrations_table()

        async with self.pool.acquire() as conn:
            result = await conn.fetchval("SELECT MAX(version) FROM _pgfast_migrations")
            return result if result is not None else 0

    async def get_applied_migrations(self) -> list[int]:
        """Get list of applied migration versions.

        Returns:
            Sorted list of applied migration versions
        """
        await self._ensure_migrations_table()

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT version FROM _pgfast_migrations ORDER BY version"
            )
            return [row["version"] for row in rows]

    async def get_pending_migrations(self) -> list[Migration]:
        """Get list of pending (unapplied) migrations.

        Returns:
            List of Migration objects that haven't been applied yet
        """
        all_migrations = self._discover_migrations()
        applied = await self.get_applied_migrations()
        applied_set = set(applied)

        return [m for m in all_migrations if m.version not in applied_set]

    async def migrate_up(self, target: int | None = None) -> list[int]:
        """Apply pending migrations up to target version.

        Args:
            target: Target version to migrate to (None = apply all pending)

        Returns:
            List of migration versions that were applied

        Raises:
            MigrationError: If migration execution fails
        """
        await self._ensure_migrations_table()

        pending = await self.get_pending_migrations()

        if not pending:
            logger.info("No pending migrations to apply")
            return []

        # Filter to target version if specified
        if target is not None:
            pending = [m for m in pending if m.version <= target]

        applied = []

        for migration in pending:
            if not migration.up_file.exists():
                raise MigrationError(
                    f"Migration up file not found: {migration.up_file}"
                )

            logger.info(f"Applying migration {migration.version}: {migration.name}")

            try:
                sql_content = migration.up_file.read_text()

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Execute migration SQL
                        await conn.execute(sql_content)

                        # Record in tracking table
                        await conn.execute(
                            """
                            INSERT INTO _pgfast_migrations (version, name)
                            VALUES ($1, $2)
                            """,
                            migration.version,
                            migration.name,
                        )

                logger.info(f"Successfully applied migration {migration.version}")
                applied.append(migration.version)

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to apply migration {migration.version}: {e}")
                raise MigrationError(
                    f"Failed to apply migration {migration.version} "
                    f"({migration.name}): {e}"
                ) from e

        return applied

    async def migrate_down(
        self, target: int | None = None, steps: int = 1
    ) -> list[int]:
        """Rollback migrations down to target version or by N steps.

        Args:
            target: Target version to migrate down to (None = rollback by steps)
            steps: Number of migrations to rollback (default: 1, ignored if target set)

        Returns:
            List of migration versions that were rolled back

        Raises:
            MigrationError: If rollback fails
        """
        await self._ensure_migrations_table()

        applied = await self.get_applied_migrations()

        if not applied:
            logger.info("No migrations to rollback")
            return []

        # Determine which migrations to rollback (in reverse order)
        if target is not None:
            to_rollback = [v for v in reversed(applied) if v > target]
        else:
            to_rollback = list(reversed(applied[-steps:]))

        if not to_rollback:
            logger.info("No migrations to rollback")
            return []

        all_migrations = self._discover_migrations()
        migration_map = {m.version: m for m in all_migrations}

        rolled_back = []

        for version in to_rollback:
            migration = migration_map.get(version)

            if migration is None:
                raise MigrationError(f"Migration files not found for version {version}")

            if not migration.down_file.exists():
                raise MigrationError(
                    f"Migration down file not found: {migration.down_file}"
                )

            logger.info(f"Rolling back migration {version}: {migration.name}")

            try:
                sql_content = migration.down_file.read_text()

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Execute rollback SQL
                        await conn.execute(sql_content)

                        # Remove from tracking table
                        await conn.execute(
                            "DELETE FROM _pgfast_migrations WHERE version = $1", version
                        )

                logger.info(f"Successfully rolled back migration {version}")
                rolled_back.append(version)

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to rollback migration {version}: {e}")
                raise MigrationError(
                    f"Failed to rollback migration {version} ({migration.name}): {e}"
                ) from e

        return rolled_back

    def create_migration(self, name: str) -> tuple[Path, Path]:
        """Create a new migration file pair (up and down).

        Args:
            name: Human-readable migration name (e.g., "add_users_table")

        Returns:
            Tuple of (up_file_path, down_file_path)

        Raises:
            SchemaError: If migrations directory doesn't exist
        """
        if not self.migrations_dir.exists():
            self.migrations_dir.mkdir(parents=True, exist_ok=True)

        # Generate timestamp version
        version = datetime.now().strftime("%Y%m%d%H%M%S")

        # Sanitize name (replace spaces with underscores, remove special chars)
        clean_name = name.replace(" ", "_").lower()
        clean_name = "".join(c for c in clean_name if c.isalnum() or c == "_")

        # Create file paths
        up_file = self.migrations_dir / f"{version}_{clean_name}_up.sql"
        down_file = self.migrations_dir / f"{version}_{clean_name}_down.sql"

        # Create files with templates
        up_template = f"""-- Migration: {name}
-- Created: {datetime.now().isoformat()}
--
-- Add your UP migration SQL here

"""

        down_template = f"""-- Migration: {name} (rollback)
-- Created: {datetime.now().isoformat()}
--
-- Add your DOWN migration SQL here (should reverse the UP migration)

"""

        up_file.write_text(up_template)
        down_file.write_text(down_template)

        logger.info(f"Created migration files: {up_file.name}, {down_file.name}")

        return up_file, down_file
