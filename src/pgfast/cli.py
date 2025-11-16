"""CLI interface for pgfast."""

import asyncio
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import asyncpg
import typer
from rich.console import Console
from rich.table import Table

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import PgfastError
from pgfast.schema import SchemaManager
from pgfast.testing import TestDatabaseManager, _pool_db_names

app = typer.Typer(
    name="pgfast",
    help="pgfast - Lightweight asyncpg integration for FastAPI",
    add_completion=False,
)

migration_app = typer.Typer(help="Migration commands")
app.add_typer(migration_app, name="schema")

test_db_app = typer.Typer(help="Test database commands")
app.add_typer(test_db_app, name="test-db")

console = Console()


def get_config() -> DatabaseConfig:
    """Get database configuration from environment or defaults."""
    import os

    url = os.getenv("DATABASE_URL")
    if not url:
        console.print("[red]ERROR:[/red] DATABASE_URL environment variable not set")
        raise typer.Exit(1)

    return DatabaseConfig(
        url=url,
        migrations_dir=os.getenv("PGFAST_MIGRATIONS_DIR", "db/migrations"),
        fixtures_dir=os.getenv("PGFAST_FIXTURES_DIR", "db/fixtures"),
    )


@app.command()
def init(
    migrations_dir: str = typer.Option(
        "db/migrations", "--migrations-dir", "-m", help="Migrations directory path"
    ),
):
    """Initialize pgfast directory structure."""
    dirs = [
        Path(migrations_dir),
        Path("db/fixtures"),
    ]

    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
        console.print(f"✓ Created directory: {dir_path}")

    console.print("\n[green]Initialization complete![/green]")
    console.print("\nNext steps:")
    console.print("  1. Set DATABASE_URL environment variable")
    console.print("  2. Run 'pgfast schema create' to create your first migration")
    console.print("  3. Edit migration files and run 'pgfast schema up'")


@migration_app.command("create")
def create_migration(
    name: str = typer.Argument(..., help="Migration name"),
):
    """Create a new migration file pair."""
    try:
        config = get_config()

        # Create manager (no pool needed for file operations)
        manager = SchemaManager(
            pool=None,  # type: ignore
            migrations_dir=config.migrations_dir,
        )

        up_file, down_file = manager.create_migration(name)

        console.print("\n[green]✓ Created migration files:[/green]")
        console.print(f"  UP:   {up_file}")
        console.print(f"  DOWN: {down_file}")
        console.print("\nEdit these files to add your migration SQL.")

    except PgfastError as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


@migration_app.command("up")
def schema_up(
    target: Optional[int] = typer.Option(
        None,
        "--target",
        "-t",
        help="Target version to migrate to (default: all pending)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be applied without executing",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip checksum validation",
    ),
):
    """Apply pending migrations."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            if dry_run:
                console.print("[bold]DRY RUN MODE - No changes will be made[/bold]\n")

            # Get pending migrations to show preview
            pending = await manager.get_pending_migrations()

            if target is not None:
                pending = [m for m in pending if m.version <= target]

            if not pending:
                console.print("[yellow]No pending migrations to apply.[/yellow]")
                return

            # Show detailed preview in dry-run mode
            if dry_run:
                console.print(f"[bold]Would apply {len(pending)} migration(s):[/bold]\n")

                for migration in pending:
                    preview = manager.preview_migration(migration, "up")

                    console.print(f"[cyan]Migration {preview['version']}:[/cyan] {preview['name']}")

                    if preview['dependencies']:
                        console.print(f"  Dependencies: {', '.join(map(str, preview['dependencies']))}")

                    console.print(f"  Checksum: {preview['checksum'][:16]}...")
                    console.print(f"  SQL Preview ({preview['total_lines']} lines):")
                    console.print(f"[dim]{preview['sql_preview']}[/dim]\n")

                return

            # Apply migrations
            console.print("Checking for pending migrations...")
            applied = await manager.schema_up(target=target, dry_run=False, force=force)

            if applied:
                console.print(
                    f"\n[green]✓ Applied {len(applied)} migration(s):[/green]"
                )
                for version in applied:
                    console.print(f"  - {version}")
            else:
                console.print("[yellow]No pending migrations to apply.[/yellow]")

        except PgfastError as e:
            console.print(f"\n[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@migration_app.command("down")
def schema_down(
    steps: int = typer.Option(
        1, "--steps", "-s", help="Number of migrations to rollback"
    ),
    target: Optional[int] = typer.Option(
        None, "--target", "-t", help="Target version to rollback to"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be rolled back without executing",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip checksum validation",
    ),
):
    """Rollback migrations."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            if dry_run:
                console.print("[bold]DRY RUN MODE - No changes will be made[/bold]\n")

            # Get applied migrations to determine what would be rolled back
            applied = await manager.get_applied_migrations()

            if not applied:
                console.print("[yellow]No migrations to rollback.[/yellow]")
                return

            # Determine which migrations would be rolled back
            if target is not None:
                to_rollback_versions = [v for v in reversed(applied) if v > target]
            else:
                to_rollback_versions = list(reversed(applied[-steps:]))

            if not to_rollback_versions:
                console.print("[yellow]No migrations to rollback.[/yellow]")
                return

            # Show detailed preview in dry-run mode
            if dry_run:
                all_migrations = manager._discover_migrations()
                migration_map = {m.version: m for m in all_migrations}

                console.print(f"[bold]Would rollback {len(to_rollback_versions)} migration(s):[/bold]\n")

                for version in to_rollback_versions:
                    migration = migration_map.get(version)
                    if migration:
                        preview = manager.preview_migration(migration, "down")

                        console.print(f"[cyan]Migration {preview['version']}:[/cyan] {preview['name']}")

                        if preview['dependencies']:
                            console.print(f"  Dependencies: {', '.join(map(str, preview['dependencies']))}")

                        console.print(f"  Checksum: {preview['checksum'][:16]}...")
                        console.print(f"  SQL Preview ({preview['total_lines']} lines):")
                        console.print(f"[dim]{preview['sql_preview']}[/dim]\n")

                return

            # Rollback migrations
            console.print("Rolling back migrations...")
            rolled_back = await manager.schema_down(
                target=target, steps=steps, dry_run=False, force=force
            )

            if rolled_back:
                console.print(
                    f"\n[green]✓ Rolled back {len(rolled_back)} migration(s):[/green]"
                )
                for version in rolled_back:
                    console.print(f"  - {version}")
            else:
                console.print("[yellow]No migrations to rollback.[/yellow]")

        except PgfastError as e:
            console.print(f"\n[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@migration_app.command("status")
def migration_status():
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

            console.print(f"\n[bold]Current Version:[/bold] {current_version}")
            console.print(f"[bold]Applied Migrations:[/bold] {len(applied)}")
            console.print(f"[bold]Pending Migrations:[/bold] {len(pending)}")

            if applied:
                console.print("\n[bold]Applied:[/bold]")
                table = Table(show_header=True, header_style="bold")
                table.add_column("Version")
                table.add_column("Name")

                for version in applied:
                    # Find migration name
                    all_migrations = manager._discover_migrations()
                    migration = next(
                        (m for m in all_migrations if m.version == version), None
                    )
                    name = migration.name if migration else "unknown"
                    table.add_row(str(version), name)

                console.print(table)

            if pending:
                console.print("\n[bold]Pending:[/bold]")
                table = Table(show_header=True, header_style="bold")
                table.add_column("Version")
                table.add_column("Name")
                table.add_column("Status")

                for migration in pending:
                    status = "✓ Ready" if migration.is_complete else "✗ Incomplete"
                    table.add_row(str(migration.version), migration.name, status)

                console.print(table)

        except PgfastError as e:
            console.print(f"\n[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@migration_app.command("deps")
def migration_deps():
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
                console.print("[yellow]No migrations found.[/yellow]")
                return

            console.print("\n[bold]Migration Dependency Graph:[/bold]\n")

            # Create table
            table = Table(show_header=True, header_style="bold")
            table.add_column("Version")
            table.add_column("Name")
            table.add_column("Status")
            table.add_column("Dependencies")

            for migration in all_migrations:
                status = "[green]Applied[/green]" if migration.version in applied else "[yellow]Pending[/yellow]"

                deps = dep_graph.get(migration.version, [])
                deps_str = ", ".join(map(str, deps)) if deps else "-"

                table.add_row(
                    str(migration.version),
                    migration.name,
                    status,
                    deps_str,
                )

            console.print(table)

            # Check for circular dependencies
            cycles = manager._detect_circular_dependencies(all_migrations)
            if cycles:
                console.print("\n[red]⚠ Circular dependencies detected:[/red]")
                for v1, v2 in cycles:
                    console.print(f"  - {v1} <-> {v2}")

        except PgfastError as e:
            console.print(f"\n[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@migration_app.command("verify")
def migration_verify():
    """Verify migration file checksums."""

    async def _run():
        config = get_config()
        pool = await create_pool(config)

        try:
            manager = SchemaManager(
                pool=pool,
                migrations_dir=config.migrations_dir,
            )

            console.print("Verifying migration checksums...\n")

            results = await manager.verify_checksums()

            if results["valid"]:
                console.print("[bold]Valid checksums:[/bold]")
                for msg in results["valid"]:
                    console.print(f"  [green]✓[/green] {msg}")

            if results["invalid"]:
                console.print("\n[bold]Invalid checksums:[/bold]")
                for msg in results["invalid"]:
                    console.print(f"  [red]✗[/red] {msg}")

                console.print("\n[red]Checksum validation failed![/red]")
                console.print("Some migration files have been modified after being applied.")
                console.print("Use --force flag to override validation if needed.")
                raise typer.Exit(1)
            else:
                if results["valid"]:
                    console.print(f"\n[green]✓ All {len(results['valid'])} applied migration(s) verified successfully![/green]")
                else:
                    console.print("[yellow]No applied migrations to verify.[/yellow]")

        except PgfastError as e:
            console.print(f"\n[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@test_db_app.command("create")
def test_db_create(
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Database name"),
    template: Optional[str] = typer.Option(
        None, "--template", "-t", help="Template to clone from"
    ),
):
    """Create a test database."""

    async def _run():
        config = get_config()
        manager = TestDatabaseManager(config, template_db=template)

        pool = await manager.create_test_db(db_name=name)
        db_name = _pool_db_names.get(id(pool), name)

        console.print(f"\n[green]✓ Created test database:[/green] {db_name}")

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

        console.print(f"\nConnection URL: {base_url}/{db_name}")

        await close_pool(pool)

    asyncio.run(_run())


@test_db_app.command("load-fixtures")
def test_db_load_fixtures(
    database: str = typer.Argument(..., help="Database name"),
    fixtures: list[str] = typer.Argument(..., help="Fixture files to load"),
):
    """Load fixtures into test database."""

    async def _run():
        config = get_config()

        # Connect to specified database
        parsed = urlparse(config.url)
        test_url = parsed._replace(path=f"/{database}").geturl()
        test_config = DatabaseConfig(
            url=test_url,
            min_connections=config.min_connections,
            max_connections=config.max_connections,
        )

        pool = await create_pool(test_config)

        try:
            manager = TestDatabaseManager(config)
            fixture_paths = [Path(f) for f in fixtures]

            await manager.load_fixtures(pool, fixture_paths)

            console.print(f"\n[green]✓ Loaded {len(fixtures)} fixture(s)[/green]")
        except PgfastError as e:
            console.print(f"[red]ERROR:[/red] {e}")
            raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@test_db_app.command("list")
def test_db_list():
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
                    console.print("[yellow]No test databases found.[/yellow]")
                    return

                table = Table(show_header=True, header_style="bold")
                table.add_column("Database")
                table.add_column("Size")

                for row in rows:
                    size_mb = row["size"] / (1024 * 1024)
                    table.add_row(row["datname"], f"{size_mb:.2f} MB")

                console.print(table)

            finally:
                await admin_conn.close()

        except Exception as e:
            console.print(f"[red]ERROR:[/red] {e}")
            raise typer.Exit(1)

    asyncio.run(_run())


@test_db_app.command("cleanup")
def test_db_cleanup(
    all: bool = typer.Option(False, "--all", "-a", help="Clean up all test databases"),
    pattern: str = typer.Option(
        "pgfast_test_%", "--pattern", "-p", help="Pattern to match"
    ),
):
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
                    "SELECT datname FROM pg_database WHERE datname LIKE $1", pattern
                )

                if not rows:
                    console.print(
                        "[yellow]No test databases found to clean up.[/yellow]"
                    )
                    return

                databases = [row["datname"] for row in rows]

                if not all:
                    console.print(f"Found {len(databases)} test database(s):")
                    for db in databases:
                        console.print(f"  - {db}")

                    if not typer.confirm("\nDo you want to delete these databases?"):
                        console.print("Cancelled.")
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
                        "SELECT format('DROP DATABASE IF EXISTS %I', $1)", db_name
                    )
                    await admin_conn.execute(query)
                    console.print(f"[green]✓[/green] Dropped {db_name}")

                console.print(
                    f"\n[green]Cleaned up {len(databases)} database(s)[/green]"
                )

            finally:
                await admin_conn.close()

        except Exception as e:
            console.print(f"[red]ERROR:[/red] {e}")
            raise typer.Exit(1)

    asyncio.run(_run())


def main():
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
