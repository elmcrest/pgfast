# pgfast
[![codecov](https://codecov.io/gh/elmcrest/pgfast/graph/badge.svg?token=dFtlfgtEQx)](https://codecov.io/gh/elmcrest/pgfast)
![CI](https://github.com/elmcrest/pgfast/actions/workflows/ci_cd.yml/badge.svg)
[![PyPI version](https://img.shields.io/pypi/v/pgfast)](https://pypi.org/project/pgfast/)
[![Python versions](https://img.shields.io/pypi/pyversions/pgfast)](https://pypi.org/project/pgfast/)
[![License](https://img.shields.io/pypi/l/pgfast)](https://pypi.org/project/pgfast/)

**Lightweight asyncpg integration for FastAPI. Raw SQL. Fast tests. Zero magic.**

pgfast gives you everything you need to build FastAPI applications with PostgreSQL connection pooling, migrations, and isolated test databases without the weight of an ORM. Write SQL, own your queries, ship faster.

## Why pgfast?

- **Raw SQL**: Write the queries you want. No ORM translation layer, no query builder abstraction.
- **Fast Tests**: Template database cloning gives you isolated test databases in milliseconds, not seconds.
- **FastAPI Native**: Lifespan integration and dependency injection that feels natural.
- **Simple Migrations**: Timestamped SQL files. Up and down. That's it.
- **Built for Testing**: Pytest fixtures included. Create isolated databases, load fixtures, test in parallel.

## Installation

```bash
pip install pgfast
```

Requires Python 3.14+ and PostgreSQL (earlier versions should work, open a PR if you'd like to  add support).

## Quick Start

### 1. Set up your FastAPI app

```python
from fastapi import FastAPI, Depends
from pgfast import DatabaseConfig, create_lifespan, get_db_pool
import asyncpg

config = DatabaseConfig(url="postgresql://localhost/mydb")
app = FastAPI(lifespan=create_lifespan(config))

@app.get("/users")
async def get_users(pool: asyncpg.Pool = Depends(get_db_pool)):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT id, name FROM users")
```

### 2. Create and run migrations

```bash
# Create a migration
pgfast schema create your/module add_users_table

# Edit the generated SQL files in your/module/migrations/
# Then preview and apply migrations
export DATABASE_URL="postgresql://localhost/mydb"
pgfast schema up --dry-run  # Preview first
pgfast schema up            # Apply migrations
```

### 3. Write tests with isolated databases

```python
import pytest
from pgfast.pytest import isolated_db

async def test_user_creation(isolated_db):
    """Each test gets a fresh database fast and isolated."""
    async with isolated_db.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (name, email)
            VALUES ('Alice', 'alice@example.com')
        """)

        user = await conn.fetchrow("SELECT * FROM users WHERE name = 'Alice'")
        assert user["email"] == "alice@example.com"
```

## Features

### Connection Management
- asyncpg connection pooling with configurable size and timeouts
- Graceful startup and shutdown with FastAPI lifespan
- Connection validation on pool creation

### Schema Migrations
- Timestamped migration files: `{timestamp}_{name}_up.sql` and `_down.sql`
- Transactional migration application
- CLI for creating, applying, and rolling back migrations
- Migration status tracking
- **Dependency tracking**: Declare dependencies between migrations
- **Checksum validation**: Detect modified migrations automatically
- **Dry-run mode**: Preview changes before applying

### Test Database Management
- Isolated test databases for every test
- Template database cloning for ~10-100x faster test setup (*needs benchmarking)
- Automatic cleanup
- Fixture loading from SQL files
- Pytest fixtures ready to use

### CLI Commands

```bash
# Migration Management
pgfast schema create <module_path> <name>    # Create migration files
pgfast schema up                             # Apply pending migrations
pgfast schema up --target <version>          # Migrate to specific version
pgfast schema up --dry-run                   # Preview migrations without applying
pgfast schema up --force                     # Skip checksum validation
pgfast schema down --steps 1                 # Rollback 1 migration
pgfast schema down --target <version>        # Rollback to specific version
pgfast schema down --dry-run                 # Preview rollback
pgfast schema status                         # Show migration status
pgfast schema deps                           # Show dependency graph
pgfast schema verify                         # Verify migration checksums

# Test Database Management
pgfast test-db create                        # Create test database
pgfast test-db list                          # List test databases
pgfast test-db cleanup                       # Clean up test databases
```

## Migration Features

### Dependency Tracking

Declare dependencies between migrations using comments:

```sql
-- depends_on: 20240101000000, 20240102000000

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(255)
);
```

Migrations are automatically applied in dependency order, and circular dependencies are detected.

### Checksum Validation

Migrations are checksummed (SHA-256) when applied. pgfast automatically detects if migration files have been modified after being applied:

```bash
pgfast schema verify  # Check for modifications
pgfast schema up      # Validates checksums automatically
pgfast schema up --force  # Skip validation if needed
```

### Dry-Run Mode

Preview migrations before applying them:

```bash
pgfast schema up --dry-run    # See what would be applied
pgfast schema down --dry-run  # See what would be rolled back
```

### Organizing Migrations at Scale

As your project grows, organize migrations using subdirectories. pgfast automatically discovers migrations in nested directories:

```
db/migrations/
├── users/
│   ├── 20250101000000_create_users_table_up.sql
│   ├── 20250101000000_create_users_table_down.sql
│   └── 20250305120000_add_email_verification_up.sql
├── products/
│   └── 20250102000000_create_products_table_up.sql
├── orders/
│   └── 20250103000000_create_orders_table_up.sql
└── auth/
    └── 20250115000000_add_oauth_providers_up.sql
```

Subdirectories are discovered automatically via the `**/migrations` pattern. Dependencies work across subdirectories, and migrations are applied in timestamp order regardless of directory structure.

You can organize by:
- **Domain/Feature**: `users/`, `products/`, `orders/`
- **Release**: `v1.0/`, `v1.1/`, `v2.0/`
- **Date + Domain**: `2025-01-auth/`, `2025-02-products/`

## Testing

pgfast includes pytest fixtures for fast, isolated testing:

```python
# tests/conftest.py
from pgfast.pytest import *

# Your tests automatically get:
# - isolated_db: Fresh database per test (with template optimization)
# - db_pool_factory: Create multiple databases in one test
# - db_with_fixtures: Database with fixtures pre-loaded
```

Run tests:
```bash
export TEST_DATABASE_URL="postgresql://localhost/postgres"
pytest              # Run tests sequentially
pytest -n auto      # Run tests in parallel (recommended)
```

The test infrastructure supports parallel execution out of the box. Each test gets an isolated database, so tests can run concurrently without conflicts.

### Advanced Testing

#### Selective Fixture Loading

Instead of loading all fixtures with `db_with_fixtures`, you can load specific fixtures using the `fixture_loader` fixture:

```python
async def test_specific_feature(isolated_db, fixture_loader):
    # Load only the 'users' and 'products' fixtures
    # This will automatically load them in dependency order
    await fixture_loader(["users", "products"])
    
    async with isolated_db.acquire() as conn:
        # ...
```

#### Fixture Reusability

Fixtures are defined as SQL files following the naming convention `{version}_{name}_fixture.sql`. pgfast automatically discovers fixtures across multiple directories (e.g., `db/fixtures/`, or any directory matching `**/fixtures` pattern).

- **Auto-Discovery**: Fixtures are discovered across multiple directories automatically. You can have fixtures in `db/fixtures/`, `module_a/fixtures/`, etc.
- **Subdirectory Support**: Like migrations, fixtures support subdirectories for organization:
  ```
  db/fixtures/
  ├── users/
  │   └── 20250101000000_create_users_fixture.sql
  └── products/
      └── 20250102000000_create_products_fixture.sql
  ```
- **Version Matching**: The version number MUST match a corresponding migration version.
- **Dependency Order**: Fixtures are loaded in the same order as their corresponding migrations.
- **Reusability**: All discovered fixtures are available globally and can be used in any test via `fixture_loader` or `db_with_fixtures`.

#### Multiple Databases

Need to test cross-database interactions? Use `db_pool_factory`:

```python
async def test_multi_db(db_pool_factory):
    # Create two isolated databases
    pool1 = await db_pool_factory()
    pool2 = await db_pool_factory()
    
    try:
        # Test interaction between databases
        pass
    finally:
        # Cleanup is handled automatically, but you can be explicit
        await db_pool_factory.cleanup(pool1)
        await db_pool_factory.cleanup(pool2)
```

## Configuration

```python
from pgfast import DatabaseConfig

config = DatabaseConfig(
    url="postgresql://localhost/mydb",
    min_connections=5,
    max_connections=20,
    timeout=10.0,
    migrations_dir="db/migrations",
    fixtures_dir="db/fixtures",
)
```

Or use environment variables:
- `DATABASE_URL`: Connection string
- `PGFAST_MIGRATIONS_DIR`: Custom migrations directory
- `PGFAST_FIXTURES_DIR`: Custom fixtures directory

## Philosophy

**SQL is not the enemy.** Modern PostgreSQL is incredibly powerful. Instead of hiding it behind abstraction layers, pgfast embraces it. Write migrations in SQL. Write queries in SQL. Use PostgreSQL features directly.

**Tests should be fast.** Creating a database per test shouldn't take seconds. With template database cloning, you get isolation without the wait.

**Integration should be simple.** No complex configuration, no global state, no magic. Just functions and fixtures that do what they say.

## Development

```bash
# Run tests
pytest              # Sequential
pytest -n auto      # Parallel (faster)

# Run with coverage
pytest --cov=src/pgfast

# Run integration tests
export TEST_DATABASE_URL="postgresql://localhost/postgres"
pytest tests/integration/
pytest -n auto tests/integration/  # Parallel
```

## License

MIT

---

Built with [asyncpg](https://github.com/MagicStack/asyncpg) and [FastAPI](https://fastapi.tiangolo.com/).
