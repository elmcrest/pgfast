# pgfast

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

Requires Python 3.14+ and PostgreSQL.

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
# Initialize directories
pgfast init

# Create a migration
pgfast schema create add_users_table

# Edit the generated SQL files in db/migrations/
# Then preview and apply migrations
export DATABASE_URL="postgresql://localhost/mydb"
pgfast schema up --dry-run  # Preview first
pgfast schema up             # Apply migrations
```

### 3. Write tests with isolated databases

```python
import pytest
from pgfast.pytest import isolated_db

async def test_user_creation(isolated_db):
    """Each test gets a fresh databasefast and isolated."""
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
- Template database cloning for 10-100x faster test setup
- Automatic cleanup
- Fixture loading from SQL files
- Pytest fixtures ready to use

### CLI Commands

```bash
# Initialization
pgfast init                           # Initialize directory structure

# Migration Management
pgfast schema create <name>           # Create migration files
pgfast schema up                      # Apply pending migrations
pgfast schema up --target <version>   # Migrate to specific version
pgfast schema up --dry-run            # Preview migrations without applying
pgfast schema up --force              # Skip checksum validation
pgfast schema down --steps 1          # Rollback 1 migration
pgfast schema down --target <version> # Rollback to specific version
pgfast schema down --dry-run          # Preview rollback
pgfast schema status                  # Show migration status
pgfast schema deps                    # Show dependency graph
pgfast schema verify                  # Verify migration checksums

# Test Database Management
pgfast test-db create                 # Create test database
pgfast test-db list                   # List test databases
pgfast test-db cleanup                # Clean up test databases
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
pytest
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
pytest

# Run with coverage
pytest --cov=src/pgfast

# Run integration tests
export TEST_DATABASE_URL="postgresql://localhost/postgres"
pytest tests/integration/
```

## License

MIT

---

Built with [asyncpg](https://github.com/MagicStack/asyncpg) and [FastAPI](https://fastapi.tiangolo.com/).
