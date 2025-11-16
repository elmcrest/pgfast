"""CLI interface for pgfast."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from pgfast.config import DatabaseConfig
from pgfast.connection import close_pool, create_pool
from pgfast.exceptions import PgfastError
from pgfast.schema import SchemaManager

app = typer.Typer(
    name="pgfast",
    help="pgfast - Lightweight asyncpg integration for FastAPI",
    add_completion=False,
)

migration_app = typer.Typer(help="Migration commands")
app.add_typer(migration_app, name="migrate")

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
    console.print("  2. Run 'pgfast migrate create' to create your first migration")
    console.print("  3. Edit migration files and run 'pgfast migrate up'")


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
def migrate_up(
    target: Optional[int] = typer.Option(
        None,
        "--target",
        "-t",
        help="Target version to migrate to (default: all pending)",
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

            console.print("Checking for pending migrations...")
            applied = await manager.migrate_up(target=target)

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
def migrate_down(
    steps: int = typer.Option(
        1, "--steps", "-s", help="Number of migrations to rollback"
    ),
    target: Optional[int] = typer.Option(
        None, "--target", "-t", help="Target version to migrate down to"
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

            console.print("Rolling back migrations...")
            rolled_back = await manager.migrate_down(target=target, steps=steps)

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


def main():
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
