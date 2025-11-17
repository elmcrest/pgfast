"""CLI interface for pgfast."""

import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlparse

import asyncpg

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import PgfastError
from pgfast.schema import SchemaManager
from pgfast.testing import TestDatabaseManager, _pool_db_names

# ANSI color codes
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a simple aligned table."""
    if not rows:
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    # Print header
    header_row = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(f"{BOLD}{header_row}{RESET}")
    print("-" * (len(header_row) + len(headers) * 2))

    # Print rows
    for row in rows:
        print("  ".join(str(cell).ljust(w) for cell, w in zip(row, widths)))


def confirm(message: str) -> bool:
    """Prompt user for yes/no confirmation."""
    while True:
        response = input(f"{message} (y/n): ").lower().strip()
        if response in ("y", "yes"):
            return True
        elif response in ("n", "no"):
            return False
        else:
            print("Please enter 'y' or 'n'")


def get_config() -> DatabaseConfig:
    """Get database configuration from environment or defaults."""
    import os

    url = os.getenv("DATABASE_URL")
    if not url:
        print(f"{RED}ERROR:{RESET} DATABASE_URL environment variable not set")
        sys.exit(1)
    else:
        return DatabaseConfig(
            url=url,
            migrations_dir=os.getenv("PGFAST_MIGRATIONS_DIR", "db/migrations"),
            fixtures_dir=os.getenv("PGFAST_FIXTURES_DIR", "db/fixtures"),
        )


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize pgfast directory structure."""
    dirs = [
        Path(args.migrations_dir),
        Path("db/fixtures"),
    ]

    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {dir_path}")

    print(f"\n{GREEN}Initialization complete!{RESET}")
    print("\nNext steps:")
    print("  1. Set DATABASE_URL environment variable")
    print("  2. Run 'pgfast schema create' to create your first migration")
    print("  3. Edit migration files and run 'pgfast schema up'")


def cmd_schema_create(args: argparse.Namespace) -> None:
    """Create a new migration file pair.

    By default, new migrations automatically depend on the latest existing migration.
    Use --no-depends to create an independent migration for parallel development.
    """
    try:
        config = get_config()

        # Create manager (no pool needed for file operations)
        manager = SchemaManager(
            pool=None,  # type: ignore
            migrations_dir=config.migrations_dir,
        )

        up_file, down_file = manager.create_migration(
            args.name, auto_depend=not args.no_depends
        )

        print(f"\n{GREEN}✓ Created migration files:{RESET}")
        print(f"  UP:   {up_file}")
        print(f"  DOWN: {down_file}")

        if not args.no_depends:
            # Check if dependency was added
            content = up_file.read_text()
            if "depends_on:" in content:
                print(f"\n{DIM}Auto-dependency added to latest migration{RESET}")

        print("\nEdit these files to add your migration SQL.")

    except PgfastError as e:
        print(f"{RED}ERROR:{RESET} {e}")
        sys.exit(1)


def cmd_schema_up(args: argparse.Namespace) -> None:
    """Apply pending migrations."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            if args.dry_run:
                print(f"{BOLD}DRY RUN MODE - No changes will be made{RESET}\n")

            # Get pending migrations to show preview
            pending = await manager.get_pending_migrations()

            if args.target is not None:
                pending = [m for m in pending if m.version <= args.target]

            if not pending:
                print(f"{YELLOW}No pending migrations to apply.{RESET}")
                return

            # Show detailed preview in dry-run mode
            if args.dry_run:
                print(f"{BOLD}Would apply {len(pending)} migration(s):{RESET}\n")

                for migration in pending:
                    preview = manager.preview_migration(migration, "up")

                    print(
                        f"{CYAN}Migration {preview['version']}:{RESET} {preview['name']}"
                    )

                    if preview["dependencies"]:
                        print(
                            f"  Dependencies: {', '.join(map(str, preview['dependencies']))}"
                        )

                    print(f"  Checksum: {preview['checksum'][:16]}...")
                    print(f"  SQL Preview ({preview['total_lines']} lines):")
                    print(f"{DIM}{preview['sql_preview']}{RESET}\n")

                return

            # Apply migrations
            print("Checking for pending migrations...")
            applied = await manager.schema_up(
                target=args.target, dry_run=False, force=args.force
            )

            if applied:
                print(f"\n{GREEN}✓ Applied {len(applied)} migration(s):{RESET}")
                for version in applied:
                    print(f"  - {version}")
            else:
                print(f"{YELLOW}No pending migrations to apply.{RESET}")

        except PgfastError as e:
            print(f"\n{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_schema_down(args: argparse.Namespace) -> None:
    """Rollback migrations."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            if args.dry_run:
                print(f"{BOLD}DRY RUN MODE - No changes will be made{RESET}\n")

            # Get applied migrations to determine what would be rolled back
            applied = await manager.get_applied_migrations()

            if not applied:
                print(f"{YELLOW}No migrations to rollback.{RESET}")
                return

            # Determine which migrations would be rolled back
            if args.target is not None:
                to_rollback_versions = [v for v in reversed(applied) if v > args.target]
            else:
                to_rollback_versions = list(reversed(applied[-args.steps :]))

            if not to_rollback_versions:
                print(f"{YELLOW}No migrations to rollback.{RESET}")
                return

            # Show detailed preview in dry-run mode
            if args.dry_run:
                all_migrations = manager._discover_migrations()
                migration_map = {m.version: m for m in all_migrations}

                print(
                    f"{BOLD}Would rollback {len(to_rollback_versions)} migration(s):{RESET}\n"
                )

                for version in to_rollback_versions:
                    migration = migration_map.get(version)
                    if migration:
                        preview = manager.preview_migration(migration, "down")

                        print(
                            f"{CYAN}Migration {preview['version']}:{RESET} {preview['name']}"
                        )

                        if preview["dependencies"]:
                            print(
                                f"  Dependencies: {', '.join(map(str, preview['dependencies']))}"
                            )

                        print(f"  Checksum: {preview['checksum'][:16]}...")
                        print(f"  SQL Preview ({preview['total_lines']} lines):")
                        print(f"{DIM}{preview['sql_preview']}{RESET}\n")

                return

            # Rollback migrations
            print("Rolling back migrations...")
            rolled_back = await manager.schema_down(
                target=args.target, steps=args.steps, dry_run=False, force=args.force
            )

            if rolled_back:
                print(f"\n{GREEN}✓ Rolled back {len(rolled_back)} migration(s):{RESET}")
                for version in rolled_back:
                    print(f"  - {version}")
            else:
                print(f"{YELLOW}No migrations to rollback.{RESET}")

        except PgfastError as e:
            print(f"\n{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_schema_status(args: argparse.Namespace) -> None:
    """Show migration status."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            current_version = await manager.get_current_version()
            applied = await manager.get_applied_migrations()
            pending = await manager.get_pending_migrations()

            print(f"\n{BOLD}Current Version:{RESET} {current_version}")
            print(f"{BOLD}Applied Migrations:{RESET} {len(applied)}")
            print(f"{BOLD}Pending Migrations:{RESET} {len(pending)}")

            if applied:
                print(f"\n{BOLD}Applied:{RESET}")
                rows = []

                for version in applied:
                    # Find migration name
                    all_migrations = manager._discover_migrations()
                    migration = next(
                        (m for m in all_migrations if m.version == version), None
                    )
                    name = migration.name if migration else "unknown"
                    rows.append([str(version), name])

                print_table(["Version", "Name"], rows)

            if pending:
                print(f"\n{BOLD}Pending:{RESET}")
                rows = []

                for migration in pending:
                    status = "✓ Ready" if migration.is_complete else "✗ Incomplete"
                    rows.append([str(migration.version), migration.name, status])

                print_table(["Version", "Name", "Status"], rows)

        except PgfastError as e:
            print(f"\n{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_schema_deps(args: argparse.Namespace) -> None:
    """Show migration dependency graph."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            dep_graph = manager.get_dependency_graph()
            all_migrations = manager._discover_migrations()
            applied = set(await manager.get_applied_migrations())

            if not all_migrations:
                print(f"{YELLOW}No migrations found.{RESET}")
                return

            print(f"\n{BOLD}Migration Dependency Graph:{RESET}\n")

            # Create table
            rows = []
            for migration in all_migrations:
                status_str = (
                    f"{GREEN}Applied{RESET}"
                    if migration.version in applied
                    else f"{YELLOW}Pending{RESET}"
                )

                deps = dep_graph.get(migration.version, [])
                deps_str = ", ".join(map(str, deps)) if deps else "-"

                rows.append(
                    [
                        str(migration.version),
                        migration.name,
                        status_str,
                        deps_str,
                    ]
                )

            print_table(["Version", "Name", "Status", "Dependencies"], rows)

            # Check for circular dependencies
            cycles = manager._detect_circular_dependencies(all_migrations)
            if cycles:
                print(f"\n{RED}⚠ Circular dependencies detected:{RESET}")
                for v1, v2 in cycles:
                    print(f"  - {v1} <-> {v2}")

        except PgfastError as e:
            print(f"\n{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_schema_verify(args: argparse.Namespace) -> None:
    """Verify migration file checksums."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            print("Verifying migration checksums...\n")

            results = await manager.verify_checksums()

            if results["valid"]:
                print(f"{BOLD}Valid checksums:{RESET}")
                for msg in results["valid"]:
                    print(f"  {GREEN}✓{RESET} {msg}")

            if results["invalid"]:
                print(f"\n{BOLD}Invalid checksums:{RESET}")
                for msg in results["invalid"]:
                    print(f"  {RED}✗{RESET} {msg}")

                print(f"\n{RED}Checksum validation failed!{RESET}")
                print("Some migration files have been modified after being applied.")
                print("Use --force flag to override validation if needed.")
                sys.exit(1)
            else:
                if results["valid"]:
                    print(
                        f"\n{GREEN}✓ All {len(results['valid'])} applied migration(s) verified successfully!{RESET}"
                    )
                else:
                    print(f"{YELLOW}No applied migrations to verify.{RESET}")

        except PgfastError as e:
            print(f"\n{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_test_db_create(args: argparse.Namespace) -> None:
    """Create a test database."""

    async def _run():
        config = get_config()
        manager = TestDatabaseManager(config, template_db=args.template)

        pool = await manager.create_test_db(db_name=args.name)
        db_name = _pool_db_names.get(id(pool), args.name)

        print(f"\n{GREEN}✓ Created test database:{RESET} {db_name}")

        # Extract base URL for connection string
        parsed = urlparse(config.url)
        base_url = f"{parsed.scheme}://"
        if parsed.username:
            base_url += parsed.username
            if parsed.password:
                base_url += f":{parsed.password}"
            base_url += "@"
        base_url += f"{parsed.hostname or 'localhost'}"
        if parsed.port:
            base_url += f":{parsed.port}"

        print(f"\nConnection URL: {base_url}/{db_name}")

        await close_pool(pool)

    asyncio.run(_run())


def cmd_fixtures_load(args: argparse.Namespace) -> None:
    """Load fixtures into database."""

    async def _run():
        config = get_config()

        # Determine which fixtures to load
        if args.fixtures:
            # Load specified fixtures
            fixture_paths = [Path(f) for f in args.fixtures]
        else:
            # Load all fixtures from fixtures_dir
            fixtures_dir = Path(config.fixtures_dir)
            if not fixtures_dir.exists():
                print(
                    f"{RED}ERROR:{RESET} Fixtures directory not found: {fixtures_dir}"
                )
                sys.exit(1)

            fixture_paths = sorted(fixtures_dir.glob("*.sql"))
            if not fixture_paths:
                print(f"{YELLOW}No fixture files found in {fixtures_dir}{RESET}")
                return

        # Determine target database
        parsed = urlparse(config.url)
        if args.database:
            # Connect to specified database
            target_url = parsed._replace(path=f"/{args.database}").geturl()
            target_config = DatabaseConfig(
                url=target_url,
                min_connections=config.min_connections,
                max_connections=config.max_connections,
            )
            target_db = args.database
        else:
            # Use DATABASE_URL as-is
            target_config = config
            target_db = parsed.path.lstrip("/")

        pool = await create_pool(target_config)

        try:
            manager = TestDatabaseManager(config)

            await manager.load_fixtures(pool, fixture_paths)

            print(
                f"\n{GREEN}✓ Loaded {len(fixture_paths)} fixture(s) into {target_db}{RESET}"
            )
        except PgfastError as e:
            print(f"{RED}ERROR:{RESET} {e}")
            sys.exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


def cmd_test_db_list(args: argparse.Namespace) -> None:
    """List pgfast test databases."""

    async def _run():
        config = get_config()

        # Connect to postgres database
        parsed = urlparse(config.url)
        admin_url = parsed._replace(path="/postgres").geturl()

        try:
            admin_conn = await asyncpg.connect(admin_url, timeout=config.timeout)

            try:
                # Query for test databases
                rows = await admin_conn.fetch(
                    """
                    SELECT datname, pg_database_size(datname) as size
                    FROM pg_database
                    WHERE datname LIKE 'pgfast_test_%' OR datname LIKE 'pgfast_template_%'
                    ORDER BY datname
                    """
                )

                if not rows:
                    print(f"{YELLOW}No test databases found.{RESET}")
                    return

                table_rows = []
                for row in rows:
                    size_mb = row["size"] / (1024 * 1024)
                    table_rows.append([row["datname"], f"{size_mb:.2f} MB"])

                print_table(["Database", "Size"], table_rows)

            finally:
                await admin_conn.close()

        except Exception as e:
            print(f"{RED}ERROR:{RESET} {e}")
            sys.exit(1)

    asyncio.run(_run())


def cmd_test_db_cleanup(args: argparse.Namespace) -> None:
    """Clean up test databases."""

    async def _run():
        config = get_config()

        # Connect to postgres database
        parsed = urlparse(config.url)
        admin_url = parsed._replace(path="/postgres").geturl()

        try:
            admin_conn = await asyncpg.connect(admin_url, timeout=config.timeout)

            try:
                # Find test databases
                rows = await admin_conn.fetch(
                    "SELECT datname FROM pg_database WHERE datname LIKE $1",
                    args.pattern,
                )

                if not rows:
                    print(f"{YELLOW}No test databases found to clean up.{RESET}")
                    return

                databases = [row["datname"] for row in rows]

                if not args.all:
                    print(f"Found {len(databases)} test database(s):")
                    for db in databases:
                        print(f"  - {db}")

                    if not confirm("\nDo you want to delete these databases?"):
                        print("Cancelled.")
                        return

                # Drop each database
                for db_name in databases:
                    # Terminate connections
                    await admin_conn.execute(
                        """
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = $1 AND pid <> pg_backend_pid()
                        """,
                        db_name,
                    )

                    # Drop database using format() with %I for safe identifier escaping
                    query = await admin_conn.fetchval(
                        "SELECT format('DROP DATABASE IF EXISTS %I', $1::text)", db_name
                    )
                    await admin_conn.execute(query)
                    print(f"{GREEN}✓{RESET} Dropped {db_name}")

                print(f"\n{GREEN}Cleaned up {len(databases)} database(s){RESET}")

            finally:
                await admin_conn.close()

        except Exception as e:
            print(f"{RED}ERROR:{RESET} {e}")
            sys.exit(1)

    asyncio.run(_run())


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for pgfast CLI."""
    parser = argparse.ArgumentParser(
        prog="pgfast",
        description="pgfast - Lightweight asyncpg integration for FastAPI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command")

    # Top-level: pgfast init
    init_parser = subparsers.add_parser(
        "init", help="Initialize pgfast directory structure"
    )
    init_parser.add_argument(
        "--migrations-dir",
        "-m",
        default="db/migrations",
        help="Migrations directory path (default: db/migrations)",
    )
    init_parser.set_defaults(func=cmd_init)

    # Schema group: pgfast schema [subcommand]
    schema_parser = subparsers.add_parser("schema", help="Schema management commands")
    schema_subparsers = schema_parser.add_subparsers(
        dest="schema_command", required=True, help="Schema command"
    )

    # schema create
    create_parser = schema_subparsers.add_parser(
        "create",
        help="Create a new migration file pair",
    )
    create_parser.add_argument("name", help="Migration name")
    create_parser.add_argument(
        "--no-depends",
        action="store_true",
        help="Don't auto-depend on latest migration (for parallel development)",
    )
    create_parser.set_defaults(func=cmd_schema_create)

    # schema up
    up_parser = schema_subparsers.add_parser("up", help="Apply pending migrations")
    up_parser.add_argument(
        "--target",
        "-t",
        type=int,
        help="Target version to migrate to (default: all pending)",
    )
    up_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be applied without executing",
    )
    up_parser.add_argument(
        "--force", action="store_true", help="Skip checksum validation"
    )
    up_parser.set_defaults(func=cmd_schema_up)

    # schema down
    down_parser = schema_subparsers.add_parser("down", help="Rollback migrations")
    down_parser.add_argument(
        "--steps",
        "-s",
        type=int,
        default=1,
        help="Number of migrations to rollback (default: 1)",
    )
    down_parser.add_argument(
        "--target", "-t", type=int, help="Target version to rollback to"
    )
    down_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be rolled back without executing",
    )
    down_parser.add_argument(
        "--force", action="store_true", help="Skip checksum validation"
    )
    down_parser.set_defaults(func=cmd_schema_down)

    # schema status
    status_parser = schema_subparsers.add_parser("status", help="Show migration status")
    status_parser.set_defaults(func=cmd_schema_status)

    # schema deps
    deps_parser = schema_subparsers.add_parser(
        "deps", help="Show migration dependency graph"
    )
    deps_parser.set_defaults(func=cmd_schema_deps)

    # schema verify
    verify_parser = schema_subparsers.add_parser(
        "verify", help="Verify migration file checksums"
    )
    verify_parser.set_defaults(func=cmd_schema_verify)

    # Fixtures group: pgfast fixtures [subcommand]
    fixtures_parser = subparsers.add_parser(
        "fixtures", help="Fixture management commands"
    )
    fixtures_subparsers = fixtures_parser.add_subparsers(
        dest="fixtures_command", required=True, help="Fixtures command"
    )

    # fixtures load
    fixtures_load_parser = fixtures_subparsers.add_parser(
        "load", help="Load fixtures into database"
    )
    fixtures_load_parser.add_argument(
        "fixtures",
        nargs="*",
        help="Fixture files to load (defaults to all *.sql files in fixtures_dir)",
    )
    fixtures_load_parser.add_argument(
        "--database",
        "-d",
        help="Target database name (defaults to DATABASE_URL database)",
    )
    fixtures_load_parser.set_defaults(func=cmd_fixtures_load)

    # Test DB group: pgfast test-db [subcommand]
    test_db_parser = subparsers.add_parser("test-db", help="Test database commands")
    test_db_subparsers = test_db_parser.add_subparsers(
        dest="test_db_command", required=True, help="Test database command"
    )

    # test-db create
    test_create_parser = test_db_subparsers.add_parser(
        "create", help="Create a test database"
    )
    test_create_parser.add_argument("--name", "-n", help="Database name")
    test_create_parser.add_argument("--template", "-t", help="Template to clone from")
    test_create_parser.set_defaults(func=cmd_test_db_create)

    # test-db list
    list_parser = test_db_subparsers.add_parser(
        "list", help="List pgfast test databases"
    )
    list_parser.set_defaults(func=cmd_test_db_list)

    # test-db cleanup
    cleanup_parser = test_db_subparsers.add_parser(
        "cleanup", help="Clean up test databases"
    )
    cleanup_parser.add_argument(
        "--all", "-a", action="store_true", help="Clean up all test databases"
    )
    cleanup_parser.add_argument(
        "--pattern",
        "-p",
        default="pgfast_test_%",
        help="Pattern to match (default: pgfast_test_%%)",
    )
    cleanup_parser.set_defaults(func=cmd_test_db_cleanup)

    return parser


def main():
    """Main entry point for CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Call the appropriate command function
    args.func(args)


if __name__ == "__main__":
    main()
