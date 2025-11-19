# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

pgfast is a lightweight asyncpg integration library for FastAPI that provides schema management, migrations, and testing utilities - all with raw SQL. It emphasizes simplicity over ORM abstractions.

## Development Commands

### Testing
```bash
# Run all tests
pytest              # Sequential
pytest -n auto      # Parallel (recommended, faster)

# Run specific test file
pytest tests/unit/test_config.py

# Run with coverage
pytest --cov=src/pgfast

# Run integration tests (requires DATABASE_URL)
pytest tests/integration/
pytest -n auto tests/integration/  # Parallel

# Note: Parallel testing is fully supported via pytest-xdist.
# Each test gets an isolated database with unique naming,
# preventing conflicts during concurrent execution.
```

### CLI Usage
```bash
# Initialize directory structure
pgfast init

# Schema management (formerly called "migrate")
pgfast schema create <name>           # Create new migration files (auto-depends on latest)
pgfast schema create <name> --no-depends  # Create independent migration
pgfast schema up                       # Apply pending migrations
pgfast schema up --target <version>   # Migrate to specific version
pgfast schema up --dry-run            # Preview migrations without applying
pgfast schema up --force              # Skip checksum validation
pgfast schema down --steps 1          # Rollback 1 migration
pgfast schema down --target <version> # Rollback to specific version
pgfast schema down --dry-run          # Preview rollback
pgfast schema status                  # Show migration status
pgfast schema deps                    # Show dependency graph
pgfast schema verify                  # Verify migration checksums

# Fixture management
pgfast fixtures load <fixtures...>               # Load into DATABASE_URL database
pgfast fixtures load <fixtures...> --database <db>  # Load into specific database

# Test database management
pgfast test-db create                 # Create test database
pgfast test-db list                   # List test databases
pgfast test-db cleanup                # Clean up test databases
```

## Architecture

### Core Components

**Connection Management** (`connection.py`)
- `create_pool()`: Creates asyncpg connection pool with validation
- `close_pool()`: Gracefully closes connection pool
- Uses asyncpg directly, no ORM layer

**FastAPI Integration** (`fastapi.py`)
- `create_lifespan()`: Factory function that returns lifespan context manager for pool lifecycle
- `get_db_pool()`: FastAPI dependency to inject pool into routes
- Pool stored in `app.state.db_pool`

**Schema Management** (`schema.py`)
- `SchemaManager`: Handles migration discovery, application, and rollback
- Migration tracking table: `_pgfast_migrations` (stores version, name, checksum, applied_at)
- Migrations are timestamped with format: `{YYYYMMDDHHmmss}_{name}_up.sql` and `{YYYYMMDDHHmmss}_{name}_down.sql`
- All migrations run in transactions
- Discovers migrations by scanning `db/migrations/` directory (or configured path)
- **Dependency tracking**: Parses `-- depends_on:` comments to build dependency graph
- **Checksum validation**: SHA-256 checksums stored and validated on apply/rollback
- **Topological sorting**: Uses Kahn's algorithm to apply migrations in dependency order
- **Dry-run mode**: Preview migrations without executing SQL

**Test Database Management** (`testing.py`)
- `DatabaseTestManager`: Creates isolated test databases per test
- `create_test_pool_with_schema()`: Convenience function to create DB with schema applied
- Template database support for fast cloning: Creates `pgfast_template_*` database once per session, then clones for each test
- `_pool_db_names` registry: Maps pool `id()` to database name for cleanup tracking
- Test databases named `pgfast_test_{uuid}`

**Pytest Integration** (`pytest.py`)
- Standard fixtures for test isolation:
  - `isolated_db`: Fresh database per test (uses template)
  - `isolated_db_no_template`: Fresh database without template optimization
  - `db_pool_factory`: Factory for creating multiple databases in one test
  - `db_with_fixtures`: Database with fixtures pre-loaded
  - `fixture_loader`: Load specific fixtures by name (e.g., `await fixture_loader(["users", "products"])`)
  - `template_db`: Session-scoped template database
- Import in `conftest.py`: `from pgfast.pytest import *`

### Configuration

**DatabaseConfig** (`config.py`)
- Requires PostgreSQL URL with validation
- Pool settings: min_connections, max_connections, timeout, command_timeout
- Directory paths: migrations_dir, fixtures_dir
- Validates URL scheme and pool settings on initialization

**Environment Variables**
- `DATABASE_URL`: Required for CLI commands
- `TEST_DATABASE_URL`: Used in tests (defaults to `postgresql://localhost/postgres`)
- `PGFAST_MIGRATIONS_DIR`: Override migrations directory (default: `db/migrations`)
- `PGFAST_FIXTURES_DIR`: Override fixtures directory (default: `db/fixtures`)

### SQL Injection Protection

The codebase uses PostgreSQL's `format()` function with `%I` identifier escaping for dynamic database/table names:
```python
query = await admin_conn.fetchval(
    "SELECT format('CREATE DATABASE %I', $1::text)", db_name
)
await admin_conn.execute(query)
```

This pattern protects against SQL injection when creating/dropping databases. Use this pattern when adding similar functionality.

### Migration File Structure

Migration files are paired:
- UP file: `{timestamp}_{name}_up.sql` - applies changes
- DOWN file: `{timestamp}_{name}_down.sql` - reverts changes

Both files must exist for a migration to be considered "complete" (`Migration.is_complete` property).

**Auto-Dependency**: By default, new migrations automatically depend on the latest existing migration. This ensures proper ordering while allowing flexibility. Use `--no-depends` flag when creating migrations for parallel development.

**Dependency Declaration**: Dependencies are declared in SQL comments at the top of the UP file:
```sql
-- depends_on: 20240101000000

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)
);
```

The parser supports various formats (case-insensitive):
- `-- depends_on: 20240101000000`
- `--depends_on: 20240101000000`
- `-- DEPENDS_ON: 20240101000000`

Multiple dependencies can be comma-separated on one line:
```sql
-- depends_on: 20240101000000, 20240102000000
```

### Error Handling

Custom exceptions in `exceptions.py`:
- `PgfastError`: Base exception
- `ValidationError`: Invalid configuration
- `ConnectionError`: Pool/connection failures
- `MigrationError`: Migration execution failures
- `SchemaError`: Schema management errors (e.g., missing migrations directory)
- `TestDatabaseError`: Test database lifecycle errors
- `DependencyError`: Migration dependency violations (circular, missing, unapplied)
- `ChecksumError`: Migration file checksum mismatches

All asyncpg exceptions are caught and wrapped in appropriate pgfast exceptions.

## Testing Patterns

### Fixture Usage in Tests

```python
# Most tests should use isolated_db
async def test_something(isolated_db):
    async with isolated_db.acquire() as conn:
        result = await conn.fetchval("SELECT 1")
        assert result == 1

# Load specific fixtures by name (maintains dependency order)
async def test_with_selective_fixtures(isolated_db, fixture_loader):
    await fixture_loader(["users", "products"])
    async with isolated_db.acquire() as conn:
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        assert user_count > 0

# Load all fixtures automatically
async def test_with_all_fixtures(db_with_fixtures):
    async with db_with_fixtures.acquire() as conn:
        # All fixtures are loaded
        pass

# For tests needing multiple databases
async def test_multi_db(db_pool_factory):
    pool1 = await db_pool_factory()
    pool2 = await db_pool_factory()
    # ... test logic
    await db_pool_factory.cleanup(pool1)
    await db_pool_factory.cleanup(pool2)
```

### Test Organization

- `tests/unit/`: Unit tests for individual components (config, exceptions, models)
- `tests/integration/`: Integration tests requiring database (connection, migrations, FastAPI integration)
- `tests/conftest.py`: Project-specific fixture configuration and overrides

## Important Implementation Details

1. **Pool ID Tracking**: Since asyncpg.Pool doesn't support weak references or custom attributes, test database names are tracked in `_pool_db_names` dict using `id(pool)` as key.

2. **Admin Operations**: Database creation/destruction requires connecting to the `postgres` admin database, not the target database.

3. **Transaction Safety**: All migrations run within transactions for atomic application/rollback.

4. **Template Optimization**: Session-scoped template database is created once with all migrations applied, then cloned for each test. This significantly speeds up test execution.

5. **Async Context Managers**: The lifespan pattern uses `@asynccontextmanager` for proper startup/shutdown sequencing.

6. **Type Casting in SQL**: When using PostgreSQL's `format()` function with asyncpg parameters, explicit type casting is required: `$1::text` instead of just `$1` to avoid `IndeterminateDatatypeError`.

7. **Dependency Validation**: When applying migrations, the validation checks that dependencies exist in either the applied set OR the pending set. The topological sort ensures correct ordering. During rollback, dependencies are validated linearly (can't rollback if remaining migrations depend on it).

8. **Checksum Algorithm**: SHA-256 checksums are calculated from the UP file content only. Checksums are stored in the `_pgfast_migrations` table and validated before applying new migrations or rolling back.

9. **Migration Discovery**: The `_discover_migrations()` method scans the filesystem for paired migration files. Only migrations with both UP and DOWN files present are considered complete (`Migration.is_complete`).

10. **Circular Dependency Detection**: Uses depth-first search (DFS) to detect cycles in the dependency graph before applying migrations.

## Dependencies

- **asyncpg**: PostgreSQL async driver
- **fastapi**: Web framework (for integration)
- **typer**: CLI framework
- **rich**: Terminal formatting for CLI
- **pytest**: Test framework
- **pytest-asyncio**: Async test support
- **pytest-xdist**: Parallel test execution (dev dependency)
- **pydantic**: Data validation (Migration model)
