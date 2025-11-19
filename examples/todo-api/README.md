# Todo API - pgfast Example

A simple but complete Todo CRUD API demonstrating all major pgfast features.

## What's Demonstrated

### Core Features
- **FastAPI Integration**: Lifespan context manager for pool management
- **Dependency Injection**: `get_db_pool()` dependency for route handlers
- **Raw SQL**: All queries written in plain SQL (no ORM)
- **Connection Pooling**: asyncpg pool managed by pgfast

### Migration Features
- **Dependency Tracking**: `create_todos` migration depends on `create_users`
- **Schema Management**: Users and todos tables with proper constraints
- **Complex SQL**: Triggers for auto-updating `updated_at` timestamps

### Testing Features
- **Isolated Databases**: Each test gets a fresh database via template cloning
- **Selective Fixtures**: Load only the fixtures you need with `fixture_loader`
- **Bulk Fixtures**: Load all fixtures automatically with `db_with_fixtures`
- **API Testing**: Test FastAPI endpoints with `api_client` fixture (httpx integration)
- **Parallel Execution**: Run tests concurrently with `pytest -n auto`
- **Fast Setup**: Template database optimization makes tests blazingly fast

## Project Structure

```
todo-api/
├── app.py                       # FastAPI application
├── db/
│   ├── migrations/              # Database migrations
│   │   ├── 20250101000000_create_users_up.sql
│   │   ├── 20250101000000_create_users_down.sql
│   │   ├── 20250102000000_create_todos_up.sql    # depends_on: users
│   │   └── 20250102000000_create_todos_down.sql
│   └── fixtures/                # Test fixtures
│       ├── 20250101000000_create_users_fixture.sql
│       └── 20250102000000_create_todos_fixture.sql
└── tests/
    ├── conftest.py              # Test configuration (includes api_client)
    ├── test_todos.py            # Database-level tests
    └── test_api.py              # API endpoint tests
```

## Quick Start

### 1. Install Dependencies

```bash
# Runtime dependencies
pip install pgfast fastapi uvicorn

# Testing dependencies
pip install pytest pytest-asyncio pytest-xdist httpx
```

### 2. Set Up Database

```bash
# Create your database
createdb todo_api

# Set database URL
export DATABASE_URL="postgresql://localhost/todo_api"

# Run migrations
cd examples/todo-api
pgfast schema up
```

### 3. Run the API

```bash
uvicorn app:app --reload
```

Visit http://localhost:8000/docs for the interactive API documentation.

## API Endpoints

### Todo Operations

```bash
# List all todos
curl http://localhost:8000/todos

# Filter by user
curl "http://localhost:8000/todos?user_id=1"

# Filter by completion status
curl "http://localhost:8000/todos?completed=false"

# Get specific todo
curl http://localhost:8000/todos/1

# Create todo
curl -X POST http://localhost:8000/todos \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "title": "New task", "completed": false}'

# Update todo (toggle completion)
curl -X PATCH http://localhost:8000/todos/1 \
  -H "Content-Type: application/json" \
  -d '{"completed": true}'

# Delete todo
curl -X DELETE http://localhost:8000/todos/1

# Get user's todos
curl http://localhost:8000/users/1/todos
```

## Running Tests

The test suite includes two files showcasing different pgfast testing patterns:
- `test_todos.py` - Database-level testing (direct SQL queries)
- `test_api.py` - API endpoint testing (HTTP requests)

### Run All Tests (Sequential)

```bash
export TEST_DATABASE_URL="postgresql://localhost/postgres"
pytest tests/ -v
```

### Run Tests in Parallel (Recommended)

```bash
pytest tests/ -n auto -v
```

Each test gets an isolated database, so parallel execution is safe and fast!

### Run Specific Test Files

```bash
# Database-level tests
pytest tests/test_todos.py -v

# API endpoint tests
pytest tests/test_api.py -v

# Run specific test classes
pytest tests/test_api.py::TestTodoEndpoints -v
pytest tests/test_api.py::TestEndToEndWorkflow -v
```

### Run Specific Test Classes (Database Tests)

```bash
# Tests with isolated databases (no fixtures)
pytest tests/test_todos.py::TestTodosWithIsolatedDB -v

# Tests with selective fixture loading
pytest tests/test_todos.py::TestTodosWithSelectiveFixtures -v

# Tests with all fixtures loaded
pytest tests/test_todos.py::TestTodosWithAllFixtures -v

# Tests demonstrating parallel execution
pytest tests/test_todos.py::TestParallelExecution -n auto -v
```

## Test Patterns Explained

### Pattern 1: Isolated DB (Clean Slate)

```python
async def test_create_user(isolated_db):
    """Each test gets a fresh database with schema applied."""
    async with isolated_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (username, email) VALUES ($1, $2)",
            "testuser", "test@example.com"
        )
        # Test your logic...
```

**Use when:** You need a clean database without pre-loaded data.

### Pattern 2: Selective Fixture Loading

```python
async def test_with_users_only(isolated_db, fixture_loader):
    """Load only the fixtures you need."""
    await fixture_loader(["create_users"])  # Just users, no todos

    async with isolated_db.acquire() as conn:
        # Test with user data...
```

**Use when:** You need specific test data but not the entire dataset.

### Pattern 3: All Fixtures Loaded

```python
async def test_with_everything(db_with_fixtures):
    """All fixtures loaded automatically."""
    async with db_with_fixtures.acquire() as conn:
        # Test with full dataset...
```

**Use when:** Your test needs the complete dataset.

### Pattern 4: API Endpoint Testing

```python
async def test_create_todo_endpoint(api_client, fixture_loader):
    """Test FastAPI endpoints with isolated database."""
    await fixture_loader(["create_users"])

    response = await api_client.post(
        "/todos",
        json={"user_id": 1, "title": "Test todo"}
    )
    assert response.status_code == 201
    assert response.json()["title"] == "Test todo"
```

**Use when:** You want to test your FastAPI endpoints directly with HTTP requests.

The `api_client` fixture (provided by pgfast's `test_client`) automatically:
- Creates an isolated test database for each test
- Overrides the `get_db_pool` dependency to use the test database
- Provides an `httpx.AsyncClient` for making HTTP requests
- Cleans up the database after the test

This is the **recommended pattern** for integration testing with FastAPI!

## Migration Management

### Create New Migration

```bash
# Create migration files
pgfast schema create add_tags_table

# Edit the generated SQL files in db/migrations/

# Preview changes
pgfast schema up --dry-run

# Apply migration
pgfast schema up
```

### Check Migration Status

```bash
# Show applied and pending migrations
pgfast schema status

# View dependency graph
pgfast schema deps

# Verify checksums
pgfast schema verify
```

### Rollback Migrations

```bash
# Preview rollback
pgfast schema down --dry-run

# Rollback one migration
pgfast schema down --steps 1

# Rollback to specific version
pgfast schema down --target 20250101000000
```

## Key Takeaways

1. **Raw SQL is powerful**: Direct control over queries without ORM overhead
2. **Template cloning is fast**: Tests run quickly even with complex schemas
3. **Isolation enables parallelism**: Every test gets its own database
4. **Fixtures follow migrations**: Dependency tracking works for fixtures too
5. **Simple beats complex**: Clean patterns, no magic, easy to understand

## Tips for Your Own App

1. **Use migrations for schema**: Let pgfast track your database structure
2. **Write raw SQL**: Take advantage of PostgreSQL's full feature set
3. **Load selective fixtures**: Only load the data your test needs
4. **Run tests in parallel**: Use `pytest -n auto` for faster feedback
5. **Use the lifespan pattern**: Let pgfast manage your connection pool lifecycle

## Powered By

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [asyncpg Documentation](https://magicstack.github.io/asyncpg/)
