import os

import pytest

from pgfast import DatabaseConfig, SchemaManager

# Import all standard fixtures from pgfast.pytest
from pgfast.pytest import (
    db_config as base_db_config,
    db_pool_factory,
    db_with_fixtures,
    fixtures_dir,
    isolated_db,
    isolated_db_no_template,
    migrations_dir,
    template_db,
)


@pytest.fixture(scope="session")
def db_config():
    """Test database configuration.

    Override the default configuration to use TEST_DATABASE_URL env var
    or default to localhost postgres database.
    """
    url = os.getenv("TEST_DATABASE_URL", "postgresql://localhost/postgres")
    return DatabaseConfig(
        url=url,
        min_connections=2,
        max_connections=5,
    )


@pytest.fixture
async def manager(isolated_db, tmp_path):
    """Create SchemaManager with temporary directories.

    Uses isolated_db fixture which provides a clean test database.
    """
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    return SchemaManager(
        pool=isolated_db,
        migrations_dir=str(migrations_dir),
    )


# For backwards compatibility, alias isolated_db as db_pool
@pytest.fixture
async def db_pool(isolated_db):
    """Provide a clean database pool.

    This is an alias for isolated_db for backwards compatibility
    with existing tests.
    """
    return isolated_db


