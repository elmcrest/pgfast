import os

import pytest

from pgfast import DatabaseConfig, SchemaManager

# Import all standard fixtures from pgfast.pytest
from pgfast.pytest import (
    db_config as base_db_config,  # noqa: F401
)
from pgfast.pytest import (
    db_pool_factory,  # noqa: F401
    db_with_fixtures,  # noqa: F401
    fixtures_dir,  # noqa: F401
    isolated_db,  # noqa: F401
    isolated_db_no_template,  # noqa: F401
    migrations_dir,  # noqa: F401
    template_db,  # noqa: F401
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
async def manager(isolated_db, tmp_path):  # noqa: F811
    """Create SchemaManager with temporary directories.

    Uses isolated_db fixture which provides a clean test database.
    """
    migrations_dir = tmp_path / "migrations"  # noqa: F811
    migrations_dir.mkdir()

    return SchemaManager(
        pool=isolated_db,
        migrations_dir=str(migrations_dir),
    )
