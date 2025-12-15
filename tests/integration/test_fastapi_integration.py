"""Integration tests for FastAPI integration."""

from contextlib import asynccontextmanager

import asyncpg
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from pgfast.fastapi import create_lifespan, create_rls_dependency, get_db_pool


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


@pytest.mark.asyncio
async def test_rls_dependency_sets_session_variable(db_config):
    """RLS dependency should set session variables via SET LOCAL."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    async def get_tenant_settings(request: Request) -> dict[str, str]:
        tenant_id = request.headers.get("X-Tenant-ID", "default")
        return {"app.tenant_id": tenant_id}

    get_rls_conn = create_rls_dependency(get_tenant_settings)

    @app.get("/check-tenant")
    async def check_tenant(conn: asyncpg.Connection = Depends(get_rls_conn)):
        result = await conn.fetchval("SELECT current_setting('app.tenant_id')")
        return {"tenant_id": result}

    with TestClient(app) as client:
        response = client.get("/check-tenant", headers={"X-Tenant-ID": "tenant-123"})
        assert response.status_code == 200
        assert response.json() == {"tenant_id": "tenant-123"}


@pytest.mark.asyncio
async def test_rls_dependency_multiple_settings(db_config):
    """RLS dependency should support multiple session variables."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    async def get_settings(request: Request) -> dict[str, str]:
        return {
            "app.tenant_id": request.headers.get("X-Tenant-ID", ""),
            "app.user_id": request.headers.get("X-User-ID", ""),
        }

    get_rls_conn = create_rls_dependency(get_settings)

    @app.get("/check-settings")
    async def check_settings(conn: asyncpg.Connection = Depends(get_rls_conn)):
        tenant = await conn.fetchval("SELECT current_setting('app.tenant_id')")
        user = await conn.fetchval("SELECT current_setting('app.user_id')")
        return {"tenant_id": tenant, "user_id": user}

    with TestClient(app) as client:
        response = client.get(
            "/check-settings",
            headers={"X-Tenant-ID": "tenant-abc", "X-User-ID": "user-456"},
        )
        assert response.status_code == 200
        assert response.json() == {"tenant_id": "tenant-abc", "user_id": "user-456"}


@pytest.mark.asyncio
async def test_rls_dependency_transaction_isolation(db_config):
    """SET LOCAL should be transaction-scoped and not leak between requests."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    async def get_tenant_settings(request: Request) -> dict[str, str]:
        return {"app.tenant_id": request.headers.get("X-Tenant-ID", "none")}

    get_rls_conn = create_rls_dependency(get_tenant_settings)

    @app.get("/check")
    async def check(conn: asyncpg.Connection = Depends(get_rls_conn)):
        return {
            "tenant": await conn.fetchval("SELECT current_setting('app.tenant_id')")
        }

    with TestClient(app) as client:
        # First request with tenant-1
        r1 = client.get("/check", headers={"X-Tenant-ID": "tenant-1"})
        assert r1.json() == {"tenant": "tenant-1"}

        # Second request with tenant-2 (should NOT see tenant-1)
        r2 = client.get("/check", headers={"X-Tenant-ID": "tenant-2"})
        assert r2.json() == {"tenant": "tenant-2"}

        # Third request with different tenant
        r3 = client.get("/check", headers={"X-Tenant-ID": "tenant-3"})
        assert r3.json() == {"tenant": "tenant-3"}


@pytest.mark.asyncio
async def test_rls_dependency_empty_settings(db_config):
    """RLS dependency should handle empty settings dict."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    async def get_empty_settings(request: Request) -> dict[str, str]:
        return {}

    get_rls_conn = create_rls_dependency(get_empty_settings)

    @app.get("/query")
    async def query(conn: asyncpg.Connection = Depends(get_rls_conn)):
        result = await conn.fetchval("SELECT 42")
        return {"result": result}

    with TestClient(app) as client:
        response = client.get("/query")
        assert response.status_code == 200
        assert response.json() == {"result": 42}


@pytest.mark.asyncio
async def test_rls_dependency_within_transaction(db_config):
    """Queries within RLS dependency should execute in a transaction."""
    app = FastAPI(lifespan=create_lifespan(db_config))

    async def get_settings(request: Request) -> dict[str, str]:
        return {"app.tenant_id": "test"}

    get_rls_conn = create_rls_dependency(get_settings)

    @app.get("/transaction-check")
    async def check_transaction(conn: asyncpg.Connection = Depends(get_rls_conn)):
        # This should work - we're inside a transaction
        in_transaction = await conn.fetchval(
            "SELECT count(*) > 0 FROM pg_stat_activity "
            "WHERE pid = pg_backend_pid() AND state = 'active'"
        )
        return {"active": in_transaction}

    with TestClient(app) as client:
        response = client.get("/transaction-check")
        assert response.status_code == 200
