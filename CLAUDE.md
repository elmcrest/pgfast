# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

pgfast is a lightweight asyncpg integration library for FastAPI that provides schema management, migrations, and testing utilities - all with raw SQL. It emphasizes simplicity over ORM abstractions.

## Development Commands

### Configuration

pgfast supports two ways to configure database connections:

**Option 1: DATABASE_URL (recommended)**
```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
pgfast schema up
```

**Option 2: POSTGRES_* fragments (Docker-style)**
```bash
# Useful for Docker Compose, Kubernetes, CI/CD
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="5432"
export POSTGRES_USER="myuser"
export POSTGRES_PASSWORD="mypass"
export POSTGRES_DB="mydb"
pgfast schema up
```

**Testing configuration**
```bash
# Option 1: URL-based
export TEST_DATABASE_URL="postgresql://localhost/postgres"

# Option 2: Fragment-based
export TEST_POSTGRES_HOST="localhost"
export TEST_POSTGRES_DB="postgres"
```

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

# Parallel Testing Considerations:
# pgfast fully supports parallel testing via pytest-xdist. Each test gets
# an isolated database with unique naming, preventing conflicts during
# concurrent execution.
#
# Template database optimization includes automatic retry (10 attempts with
# exponential backoff from 10-100ms) when template is locked, with fallback
# to no-template creation only if retries fail.
#
# IMPORTANT: When running tests in parallel, monitor database resource usage.
# Each parallel test creates a temporary database, which consumes:
# - Connection slots (each pool uses min_connections to max_connections)
# - Background workers (especially with extensions like TimescaleDB, PostGIS)
# - Memory (shared buffers, work_mem per connection)
# - Disk I/O (checkpoints, WAL writes)
#
# If tests become slow with high parallelism:
# 1. Check postgres logs for resource warnings (e.g., "out of background workers")
# 2. Reduce parallelism: pytest -n 4 (instead of -n auto)
# 3. Increase database limits (max_connections, max_worker_processes, etc.)
# 4. For TimescaleDB specifically: timescaledb.max_background_workers
#
# Rule of thumb: Start with pytest -n 4 and scale up while monitoring performance.
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
- `create_rls_dependency()`: Factory for creating RLS-aware connection dependencies with session variables
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
- **Intelligent retry and fallback for parallel testing**: When template database is locked (parallel pytest-xdist execution), automatically retries the fast template approach up to 10 times with exponential backoff (10ms → 20ms → 40ms → 80ms, capped at 100ms). If all retries fail, falls back to no-template creation with direct migration application. This provides optimal performance: most tests get fast cloning, brief conflicts are resolved by retry, and only sustained resource exhaustion triggers the slower fallback.
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
- Directory paths: migrations_dirs, fixtures_dirs (optional - auto-discovers if not set)
- Auto-discovery patterns: `**/migrations`, `**/fixtures` (configurable)
- Exclude patterns: Directories excluded from auto-discovery (default: `examples`, `node_modules`, `.venv`, `venv`, `.git`, `.pytest_cache`, `__pycache__`, `dist`, `build`)
- Validates URL scheme and pool settings on initialization

**Environment Variables**

Database configuration (priority order):
1. `DATABASE_URL`: PostgreSQL connection URL (recommended, e.g., `postgresql://user:pass@host:5432/dbname`)
2. `POSTGRES_*` fragments (Docker-style): Alternative to DATABASE_URL
   - `POSTGRES_HOST` (default: `localhost`)
   - `POSTGRES_PORT` (default: `5432`)
   - `POSTGRES_USER` (default: `postgres`)
   - `POSTGRES_PASSWORD` (optional)
   - `POSTGRES_DB` (required if using fragments)

Testing configuration (priority order):
1. `TEST_DATABASE_URL`: Test database URL (defaults to `postgresql://localhost/postgres`)
2. `TEST_POSTGRES_*` fragments: Same as above but with `TEST_` prefix

Directory overrides:
- `PGFAST_MIGRATIONS_DIRS`: Colon-separated migration directories (overrides auto-discovery)
- `PGFAST_FIXTURES_DIRS`: Colon-separated fixture directories (overrides auto-discovery)

**Note**: `DATABASE_URL` takes priority over `POSTGRES_*` fragments if both are set. The fragment approach is useful for Docker Compose, Kubernetes, and CI/CD environments where credentials are managed separately.

### SQL Injection Protection

The codebase uses PostgreSQL's `format()` function with `%I` identifier escaping for dynamic database/table names:
```python
query = await admin_conn.fetchval(
    "SELECT format('CREATE DATABASE %I', $1::text)", db_name
)
await admin_conn.execute(query)
```

This pattern protects against SQL injection when creating/dropping databases. Use this pattern when adding similar functionality.

### Row-Level Security (RLS) Support

pgfast provides `create_rls_dependency()` for multi-tenant applications using PostgreSQL RLS policies. It sets session variables per-request using `SET LOCAL` within a transaction, ensuring:

- **PgBouncer compatibility**: Uses `set_config()` with `LOCAL` scope, so settings don't leak between clients in transaction pooling mode
- **Automatic cleanup**: Variables are transaction-scoped and automatically reset when the request ends
- **Injection safety**: Uses parameterized queries via PostgreSQL's `set_config()` function

**Usage**:
```python
from fastapi import Depends, Request
from pgfast import create_rls_dependency, create_lifespan, DatabaseConfig

config = DatabaseConfig(url="postgresql://localhost/mydb")
app = FastAPI(lifespan=create_lifespan(config))

async def get_tenant_settings(request: Request) -> dict[str, str]:
    # Extract tenant from JWT, header, etc.
    tenant_id = request.headers.get("X-Tenant-ID", "")
    return {"app.tenant_id": tenant_id}

get_rls_connection = create_rls_dependency(get_tenant_settings)

@app.get("/items")
async def list_items(conn: asyncpg.Connection = Depends(get_rls_connection)):
    # RLS policies using current_setting('app.tenant_id') work here
    return await conn.fetch("SELECT * FROM items")
```

**Multiple settings**:
```python
async def get_rls_settings(request: Request) -> dict[str, str]:
    return {
        "app.tenant_id": request.state.tenant_id,
        "app.user_id": request.state.user_id,
        "app.role": request.state.role,
    }
```

**Note**: All queries execute inside a transaction (required for `SET LOCAL`). For read-only queries this has no practical impact. For write operations, be aware you're already in a transaction context.

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

**Subdirectory Organization**: Migrations can be organized in subdirectories for better scalability:
```
db/migrations/
├── users/
│   ├── 20250101000000_create_users_table_up.sql
│   └── 20250101000000_create_users_table_down.sql
├── products/
│   └── 20250102000000_create_products_table_up.sql
└── orders/
    └── 20250103000000_create_orders_table_up.sql
```

The `**/migrations` pattern automatically discovers migrations in nested directories. Dependencies work across subdirectories, and migrations are applied in dependency order.

### Fixture File Structure

Fixtures follow the naming convention `{version}_{name}_fixture.sql` where the version matches a migration version. Like migrations, fixtures support subdirectory organization:

```
db/fixtures/
├── users/
│   └── 20250101000000_create_users_fixture.sql
└── products/
    └── 20250102000000_create_products_fixture.sql
```

Fixtures are discovered via the `**/fixtures` pattern (testing.py:433 uses `glob("**/*.sql")`). They inherit dependency order from their corresponding migrations and are loaded in that order to ensure referential integrity.

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

4. **Template Optimization with Retry and Fallback**: Session-scoped template database is created once with all migrations applied, then cloned for each test. This significantly speeds up test execution. If the template is locked during parallel test execution (pytest-xdist), the system automatically retries up to 10 times with exponential backoff (10ms → 20ms → 40ms → 80ms, capped at 100ms). Only if all retries fail does it fall back to creating databases without the template and applying migrations directly. This three-tier approach (fast clone → retry → fallback) provides optimal performance in most scenarios while maintaining reliability under sustained database resource exhaustion.

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
