"""Schema management for pgfast."""

import logging
from datetime import datetime
from pathlib import Path

import asyncpg

from pgfast.config import DatabaseConfig
from pgfast.exceptions import (
    ChecksumError,
    DependencyError,
    MigrationError,
    SchemaError,
)
from pgfast.migrations import Migration

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database migrations.

    Args:
        pool: asyncpg connection pool
        config: DatabaseConfig with migrations directories configuration
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        config: DatabaseConfig,
    ):
        self.pool = pool
        self.config = config
        self.migrations_dirs = config.discover_migrations_dirs()

    def _discover_in_directory(self, migrations_dir: Path) -> list[Migration]:
        """Discover migrations in a single directory and its subdirectories.

        Args:
            migrations_dir: Path to migrations directory

        Returns:
            List of Migration objects from this directory

        Raises:
            MigrationError: If migration files are malformed
        """
        if not migrations_dir.exists():
            return []

        # Find all _up.sql files recursively (supports subdirectory organization)
        up_files = list(migrations_dir.glob("**/*_up.sql"))

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
                    version=version,
                    name=name,
                    up_file=up_file,
                    down_file=down_file,
                    source_dir=migrations_dir,
                )
            )

        return migrations

    def _discover_migrations(self) -> list[Migration]:
        """Discover all migrations across all configured directories.

        Returns:
            List of Migration objects sorted by version

        Raises:
            MigrationError: If migration files are malformed or version conflicts exist
        """
        all_migrations = []
        version_sources: dict[
            int, Path
        ] = {}  # Track version -> source_dir for conflict detection

        # Deduplicate directories to avoid scanning the same directory twice
        seen_dirs = set()
        unique_dirs = []
        for d in self.migrations_dirs:
            resolved = d.resolve() if isinstance(d, Path) else Path(d).resolve()
            if resolved not in seen_dirs:
                seen_dirs.add(resolved)
                unique_dirs.append(resolved)

        for migrations_dir in unique_dirs:
            for migration in self._discover_in_directory(migrations_dir):
                if migration.version in version_sources:
                    raise MigrationError(
                        f"Version conflict: {migration.version} found in both "
                        f"{version_sources[migration.version]} and {migrations_dir}"
                    )
                version_sources[migration.version] = migrations_dir
                all_migrations.append(migration)

        # Sort by version
        return sorted(all_migrations, key=lambda m: m.version)

    async def _ensure_migrations_table(self) -> None:
        """Create migrations tracking table if it doesn't exist.

        The table schema includes:
        - version: Migration version number (primary key)
        - name: Migration name
        - checksum: SHA-256 hash of migration file contents
        - applied_at: Timestamp when migration was applied
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS _pgfast_migrations (
                    version BIGINT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    checksum VARCHAR(64) NOT NULL,
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

    async def get_migration_checksums(self) -> dict[int, str]:
        """Get checksums of applied migrations from database.

        Returns:
            Dictionary mapping migration version to checksum
        """
        await self._ensure_migrations_table()

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT version, checksum FROM _pgfast_migrations")
            return {row["version"]: row["checksum"] for row in rows}

    def _detect_circular_dependencies(
        self, migrations: list[Migration]
    ) -> list[tuple[int, int]]:
        """Detect circular dependencies in migrations.

        Args:
            migrations: List of Migration objects to check

        Returns:
            List of circular dependency pairs (version1, version2)
        """
        # Build adjacency list
        graph: dict[int, list[int]] = {}
        for migration in migrations:
            graph[migration.version] = migration.dependencies

        # Track visited nodes and recursion stack for cycle detection
        visited: set[int] = set()
        rec_stack: set[int] = set()
        cycles: list[tuple[int, int]] = []

        def visit(version: int, path: list[int]) -> None:
            """DFS to detect cycles."""
            if version in rec_stack:
                # Found a cycle - find where it starts
                cycle_start = path.index(version)
                for i in range(cycle_start, len(path) - 1):
                    cycles.append((path[i], path[i + 1]))
                return

            if version in visited:
                return

            visited.add(version)
            rec_stack.add(version)
            path.append(version)

            for dep in graph.get(version, []):
                visit(dep, path[:])

            rec_stack.remove(version)

        for migration in migrations:
            if migration.version not in visited:
                visit(migration.version, [])

        return cycles

    def _validate_dependencies(
        self, migrations: list[Migration], applied: set[int]
    ) -> list[str]:
        """Validate migration dependencies.

        Args:
            migrations: List of migrations to validate
            applied: Set of already applied migration versions

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Check for circular dependencies
        cycles = self._detect_circular_dependencies(migrations)
        if cycles:
            for v1, v2 in cycles:
                errors.append(f"Circular dependency detected: {v1} <-> {v2}")

        # Check if dependencies are satisfied
        all_versions = {m.version for m in migrations} | applied

        for migration in migrations:
            for dep in migration.dependencies:
                if dep not in all_versions:
                    errors.append(
                        f"Migration {migration.version} depends on unknown migration {dep}"
                    )

        return errors

    def _topological_sort(self, migrations: list[Migration]) -> list[Migration]:
        """Sort migrations by dependency order using topological sort.

        Args:
            migrations: List of migrations to sort

        Returns:
            Migrations sorted in dependency order

        Raises:
            DependencyError: If circular dependencies exist
        """
        # Build adjacency list and in-degree count
        graph: dict[int, list[int]] = {m.version: [] for m in migrations}
        in_degree: dict[int, int] = {m.version: 0 for m in migrations}
        migration_map = {m.version: m for m in migrations}

        for migration in migrations:
            for dep in migration.dependencies:
                if dep in graph:  # Only consider dependencies within this set
                    graph[dep].append(migration.version)
                    in_degree[migration.version] += 1

        # Kahn's algorithm for topological sort
        queue = [v for v in in_degree if in_degree[v] == 0]
        sorted_versions = []

        while queue:
            # Sort queue by version to ensure deterministic ordering
            queue.sort()
            version = queue.pop(0)
            sorted_versions.append(version)

            for neighbor in graph[version]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check if all migrations were processed (no cycles)
        if len(sorted_versions) != len(migrations):
            raise DependencyError("Circular dependency detected in migrations")

        return [migration_map[v] for v in sorted_versions]

    async def _validate_checksums(
        self, migrations: list[Migration], force: bool = False
    ) -> list[str]:
        """Validate checksums of applied migrations.

        Args:
            migrations: List of migrations to validate
            force: If True, skip validation and return empty list

        Returns:
            List of checksum mismatch warnings (empty if valid or force=True)
        """
        if force:
            return []

        warnings = []
        stored_checksums = await self.get_migration_checksums()

        for migration in migrations:
            if migration.version in stored_checksums:
                current_checksum = migration.calculate_checksum()
                stored_checksum = stored_checksums[migration.version]

                if current_checksum != stored_checksum:
                    warnings.append(
                        f"Migration {migration.version} ({migration.name}) has been modified "
                        f"since it was applied (checksum mismatch)"
                    )

        return warnings

    def get_dependency_graph(self) -> dict[int, list[int]]:
        """Get dependency graph for all migrations.

        Returns:
            Dictionary mapping migration version to list of dependencies
        """
        migrations = self._discover_migrations()
        return {m.version: m.dependencies for m in migrations}

    async def verify_checksums(self) -> dict[str, list[str]]:
        """Verify checksums for all applied migrations.

        Returns:
            Dictionary with 'valid' and 'invalid' keys containing lists of messages
        """
        all_migrations = self._discover_migrations()
        stored_checksums = await self.get_migration_checksums()

        valid = []
        invalid = []

        for migration in all_migrations:
            if migration.version in stored_checksums:
                current_checksum = migration.calculate_checksum()
                stored_checksum = stored_checksums[migration.version]

                if current_checksum == stored_checksum:
                    valid.append(
                        f"Migration {migration.version} ({migration.name}): OK"
                    )
                else:
                    invalid.append(
                        f"Migration {migration.version} ({migration.name}): "
                        f"CHECKSUM MISMATCH (expected {stored_checksum[:8]}..., "
                        f"got {current_checksum[:8]}...)"
                    )

        return {"valid": valid, "invalid": invalid}

    def preview_migration(self, migration: Migration, direction: str = "up") -> dict:
        """Get preview information for a migration.

        Args:
            migration: Migration to preview
            direction: Either "up" or "down"

        Returns:
            Dictionary with preview information including SQL snippet
        """
        sql_content = migration.read_sql(direction)
        lines = sql_content.split("\n")

        # Get first 50 lines of SQL (excluding empty lines and comments at the start)
        sql_lines = []
        for line in lines:
            stripped = line.strip()
            # Include the line if it's not empty or if we've already started collecting
            if sql_lines or (stripped and not stripped.startswith("--")):
                sql_lines.append(line)
                if len(sql_lines) >= 50:
                    break

        sql_preview = "\n".join(sql_lines)
        if len(lines) > 50:
            sql_preview += f"\n... ({len(lines) - 50} more lines)"

        return {
            "version": migration.version,
            "name": migration.name,
            "dependencies": migration.dependencies,
            "checksum": migration.calculate_checksum(),
            "sql_preview": sql_preview,
            "total_lines": len(lines),
        }

    async def schema_up(
        self,
        target: int | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> list[int]:
        """Apply pending migrations up to target version.

        Args:
            target: Target version to migrate to (None = apply all pending)
            dry_run: If True, show what would be applied without executing
            force: If True, skip checksum validation

        Returns:
            List of migration versions that were applied (or would be applied in dry-run)

        Raises:
            MigrationError: If migration execution fails
            DependencyError: If dependency validation fails
            ChecksumError: If checksum validation fails and force=False
        """
        await self._ensure_migrations_table()

        pending = await self.get_pending_migrations()

        if not pending:
            logger.info("No pending migrations to apply")
            return []

        # Filter to target version if specified
        if target is not None:
            pending = [m for m in pending if m.version <= target]

        if not pending:
            logger.info("No pending migrations to apply")
            return []

        # Validate checksums for all migrations (including applied ones)
        all_migrations = self._discover_migrations()
        checksum_warnings = await self._validate_checksums(all_migrations, force)

        if checksum_warnings and not force:
            error_msg = "Checksum validation failed:\n" + "\n".join(checksum_warnings)
            logger.error(error_msg)
            raise ChecksumError(
                f"{error_msg}\n\nUse --force to override checksum validation."
            )

        # Validate dependencies
        applied_versions = await self.get_applied_migrations()
        applied_set = set(applied_versions)

        dep_errors = self._validate_dependencies(pending, applied_set)
        if dep_errors:
            error_msg = "Dependency validation failed:\n" + "\n".join(dep_errors)
            logger.error(error_msg)
            raise DependencyError(error_msg)

        # Sort by dependency order
        try:
            pending = self._topological_sort(pending)
        except DependencyError as e:
            logger.error(f"Failed to sort migrations: {e}")
            raise

        # Dry run mode - show what would be applied
        if dry_run:
            logger.info("DRY RUN: No changes will be made")
            for migration in pending:
                logger.info(
                    f"Would apply migration {migration.version}: {migration.name}"
                )
            return [m.version for m in pending]

        # Apply migrations
        applied = []

        for migration in pending:
            if not migration.up_file.exists():
                raise MigrationError(
                    f"Migration up file not found: {migration.up_file}"
                )

            logger.info(f"Applying migration {migration.version}: {migration.name}")

            try:
                sql_content = migration.read_sql("up")
                checksum = migration.calculate_checksum()

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Execute migration SQL
                        await conn.execute(sql_content)

                        # Record in tracking table with checksum
                        await conn.execute(
                            """
                            INSERT INTO _pgfast_migrations (version, name, checksum)
                            VALUES ($1, $2, $3)
                            """,
                            migration.version,
                            migration.name,
                            checksum,
                        )

                logger.info(f"Successfully applied migration {migration.version}")
                applied.append(migration.version)

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to apply migration {migration.version}: {e}")
                raise MigrationError(
                    f"Failed to apply migration {migration.version} "
                    f"({migration.name}): {e}",
                    applied_migrations=applied,
                ) from e

        return applied

    async def schema_down(
        self,
        target: int | None = None,
        steps: int = 1,
        dry_run: bool = False,
        force: bool = False,
    ) -> list[int]:
        """Rollback migrations down to target version or by N steps.

        Args:
            target: Target version to rollback to (None = rollback by steps)
            steps: Number of migrations to rollback (default: 1, ignored if target set)
            dry_run: If True, show what would be rolled back without executing
            force: If True, skip checksum validation

        Returns:
            List of migration versions that were rolled back (or would be in dry-run)

        Raises:
            MigrationError: If rollback fails
            DependencyError: If rollback would break dependencies
            ChecksumError: If checksum validation fails and force=False
        """
        await self._ensure_migrations_table()

        applied = await self.get_applied_migrations()

        if not applied:
            logger.info("No migrations to rollback")
            return []

        # Determine which migrations to rollback (in reverse order)
        if target is not None:
            to_rollback_versions = [v for v in reversed(applied) if v > target]
        else:
            to_rollback_versions = list(reversed(applied[-steps:]))

        if not to_rollback_versions:
            logger.info("No migrations to rollback")
            return []

        all_migrations = self._discover_migrations()
        migration_map = {m.version: m for m in all_migrations}

        # Get migration objects for rollback
        to_rollback = []
        for version in to_rollback_versions:
            migration = migration_map.get(version)
            if migration is None:
                raise MigrationError(f"Migration files not found for version {version}")
            to_rollback.append(migration)

        # Validate checksums
        checksum_warnings = await self._validate_checksums(to_rollback, force)

        if checksum_warnings and not force:
            error_msg = "Checksum validation failed:\n" + "\n".join(checksum_warnings)
            logger.error(error_msg)
            raise ChecksumError(
                f"{error_msg}\n\nUse --force to override checksum validation."
            )

        # Check if any remaining migrations depend on migrations being rolled back
        remaining_versions = set(applied) - set(to_rollback_versions)
        remaining_migrations = [
            m for m in all_migrations if m.version in remaining_versions
        ]

        dep_errors = []
        for migration in remaining_migrations:
            for dep in migration.dependencies:
                if dep in to_rollback_versions:
                    dep_errors.append(
                        f"Cannot rollback migration {dep}: "
                        f"migration {migration.version} depends on it"
                    )

        if dep_errors:
            error_msg = "Dependency validation failed:\n" + "\n".join(dep_errors)
            logger.error(error_msg)
            raise DependencyError(error_msg)

        # Dry run mode - show what would be rolled back
        if dry_run:
            logger.info("DRY RUN: No changes will be made")
            for migration in to_rollback:
                logger.info(
                    f"Would rollback migration {migration.version}: {migration.name}"
                )
            return [m.version for m in to_rollback]

        # Rollback migrations
        rolled_back = []

        for migration in to_rollback:
            if not migration.down_file.exists():
                raise MigrationError(
                    f"Migration down file not found: {migration.down_file}"
                )

            logger.info(f"Rolling back migration {migration.version}: {migration.name}")

            try:
                sql_content = migration.read_sql("down")

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Execute rollback SQL
                        await conn.execute(sql_content)

                        # Remove from tracking table
                        await conn.execute(
                            "DELETE FROM _pgfast_migrations WHERE version = $1",
                            migration.version,
                        )

                logger.info(f"Successfully rolled back migration {migration.version}")
                rolled_back.append(migration.version)

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to rollback migration {migration.version}: {e}")
                raise MigrationError(
                    f"Failed to rollback migration {migration.version} "
                    f"({migration.name}): {e}"
                ) from e

        return rolled_back

    def create_migration(
        self, name: str, target_dir: str | Path, auto_depend: bool = True
    ) -> tuple[Path, Path]:
        """Create a new migration file pair (up and down) in specified directory.

        By default, new migrations automatically depend on the latest existing migration
        across ALL migration directories. This ensures migrations are applied in the
        correct order while still allowing flexibility for parallel development
        (disable with auto_depend=False).

        Args:
            name: Human-readable migration name (e.g., "add_users_table")
            target_dir: Directory to create migration files in
            auto_depend: If True (default), automatically depend on the latest migration

        Returns:
            Tuple of (up_file_path, down_file_path)

        Raises:
            SchemaError: If target directory cannot be created
        """
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)

        # Get existing migrations across ALL directories to determine dependencies
        existing_migrations = self._discover_migrations()

        # Determine dependency comment
        dependency_comment = ""
        if auto_depend and existing_migrations:
            # Get the latest migration version across all directories
            latest = max(existing_migrations, key=lambda m: m.version)
            dependency_comment = f"-- depends_on: {latest.version}\n"

        # Generate timestamp version with milliseconds to avoid conflicts
        # Format: YYYYMMDDHHMMSSfff (17 digits)
        now = datetime.now()
        version = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"

        # Sanitize name (replace spaces with underscores, remove special chars)
        clean_name = name.replace(" ", "_").lower()
        clean_name = "".join(c for c in clean_name if c.isalnum() or c == "_")

        # Create file paths and ensure uniqueness
        up_file = target_path / f"{version}_{clean_name}_up.sql"
        down_file = target_path / f"{version}_{clean_name}_down.sql"

        # Check if ANY migration with this version exists (not just same name)
        # This prevents version conflicts when creating migrations with different names rapidly
        counter = 0
        while any(
            f.stem.startswith(version) for f in target_path.glob(f"{version}*_up.sql")
        ):
            counter += 1
            version_int = int(version) + counter
            version = str(version_int)
            up_file = target_path / f"{version}_{clean_name}_up.sql"
            down_file = target_path / f"{version}_{clean_name}_down.sql"
            if counter > 1000:  # Safety limit
                raise SchemaError("Unable to generate unique migration version")

        # Create files with templates
        up_template = f"""-- Migration: {name}
-- Created: {datetime.now().isoformat()}
--
{dependency_comment}-- Add your UP migration SQL here
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
