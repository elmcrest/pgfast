"""Test configuration for todo API.

This demonstrates pgfast's pytest integration:
- Import all standard fixtures from pgfast.pytest
- All tests get isolated databases automatically
- Template database optimization for fast test execution

IMPORTANT: This example includes some workarounds (absolute paths, sys.path
manipulation) that are only needed because this example lives inside the
pgfast project itself. In a real application, your conftest.py would be
much simpler - see the notes in the fixtures below.
"""

import getpass
import os

import pytest

from pgfast import DatabaseConfig

# Import all pgfast pytest fixtures
from pgfast.pytest import (
    db_config as base_db_config,
)
from pgfast.pytest import (
    db_pool_factory,
    db_with_fixtures,
    fixture_loader,
    isolated_db,
    isolated_db_no_template,
    template_db,
    test_client,
)

# Make fixtures available to all tests (avoid F401 warning)
__all__ = [
    "base_db_config",
    "db_pool_factory",
    "db_with_fixtures",
    "fixture_loader",
    "isolated_db",
    "isolated_db_no_template",
    "template_db",
    "test_client",
    "api_client",
]


@pytest.fixture(scope="session")
def db_config():
    """Override default config to use example app paths.

    NOTE: The absolute path logic below is only needed because this example
    lives inside the pgfast project itself. In a real application, you can
    typically use relative paths or rely on auto-discovery:

    Real-world usage (much simpler):
        return DatabaseConfig(
            url=os.getenv("DATABASE_URL"),
            # Auto-discovery will find migrations and fixtures automatically
        )

    Or with relative paths:
        return DatabaseConfig(
            url=os.getenv("DATABASE_URL"),
            migrations_dirs=["db/migrations"],
            fixtures_dirs=["db/fixtures"],
        )
    """
    from pathlib import Path

    # Get absolute path to example app root (parent of tests/)
    # Only needed because this example is nested inside the pgfast project
    example_root = Path(__file__).parent.parent

    # Default to current user's database (typical for local dev)
    default_url = f"postgresql://{getpass.getuser()}@localhost/postgres"
    url = os.getenv("TEST_DATABASE_URL", default_url)
    return DatabaseConfig(
        url=url,
        min_connections=2,
        max_connections=5,
        # Point to our example app directories using absolute paths
        migrations_dirs=[str(example_root / "db" / "migrations")],
        fixtures_dirs=[str(example_root / "db" / "fixtures")],
    )


@pytest.fixture
async def api_client(test_client):
    """FastAPI test client with isolated database.

    This fixture provides an HTTP client for testing FastAPI endpoints.
    The database pool dependency is automatically overridden to use
    an isolated test database.

    Usage:
        async def test_endpoint(api_client):
            response = await api_client.get("/todos")
            assert response.status_code == 200

    NOTE: The sys.path manipulation below is only needed because this example
    lives inside the pgfast project. In a real application, your app module
    would be properly installed or importable:

    Real-world usage (much simpler):
        from app import app, get_db_pool

        async with test_client(app, get_db_pool) as client:
            yield client
    """
    # Import app and dependency here to avoid import-time side effects
    import sys
    from pathlib import Path

    # Add parent directory to path to import app
    # Only needed because this example is nested inside the pgfast project
    app_dir = Path(__file__).parent.parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))

    from app import app, get_db_pool  # type: ignore[import-not-found]

    async with test_client(app, get_db_pool) as client:
        yield client
