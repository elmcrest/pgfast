"""Integration tests for FastAPI integration."""

import pytest
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
import asyncpg

from pgfast.config import DatabaseConfig
from pgfast.fastapi import create_lifespan, get_db_pool


@pytest.fixture
def db_config():
    """Test database configuration."""
    return DatabaseConfig(
        url="postgresql://localhost:5432/pgfast_test",
        min_connections=2,
        max_connections=5,
    )


@pytest.mark.asyncio
async def test_setup_database_lifecycle(db_config):
    """Should setup and teardown database with FastAPI lifecycle."""
    # Track lifecycle events
    startup_called = False
    shutdown_called = False
    pool_at_startup = None

    @asynccontextmanager
    async def lifespan_with_tracking(app: FastAPI):
        nonlocal startup_called, shutdown_called, pool_at_startup

        # Use the database lifespan
        async with create_lifespan(db_config)(app):
            startup_called = True
            pool_at_startup = app.state.db_pool
            assert pool_at_startup is not None

            yield

        shutdown_called = True

    app = FastAPI(lifespan=lifespan_with_tracking)

    # Create test client (triggers startup/shutdown)
    with TestClient(app):
        assert startup_called
        assert pool_at_startup is not None

    assert shutdown_called


@pytest.mark.asyncio
async def test_get_db_pool_dependency(db_config):
    """Should provide pool via dependency injection."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    @app.get("/test")
    async def test_route(pool: asyncpg.Pool = Depends(get_db_pool)):
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 42")
            return {"result": result}

    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"result": 42}


@pytest.mark.asyncio
async def test_multiple_requests_use_pool(db_config):
    """Multiple requests should reuse the same pool."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    pool_ids = []

    @app.get("/check")
    async def check_pool(pool: asyncpg.Pool = Depends(get_db_pool)):
        pool_ids.append(id(pool))
        return {"pool_id": id(pool)}

    with TestClient(app) as client:
        # Make multiple requests
        for _ in range(3):
            response = client.get("/check")
            assert response.status_code == 200

        # All requests should use the same pool
        assert len(set(pool_ids)) == 1


@pytest.mark.asyncio
async def test_pool_available_during_request(db_config):
    """Pool should be available throughout request lifecycle."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    @app.get("/query")
    async def query_route(pool: asyncpg.Pool = Depends(get_db_pool)):
        # Execute multiple queries in same request
        results = []
        for i in range(3):
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT $1::int", i)
                results.append(result)
        return {"results": results}

    with TestClient(app) as client:
        response = client.get("/query")
        assert response.status_code == 200
        assert response.json() == {"results": [0, 1, 2]}
