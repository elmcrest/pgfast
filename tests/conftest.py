import getpass
import os

import pytest

from pgfast import DatabaseConfig, SchemaManager

# Import all standard fixtures from pgfast.pytest
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
]


@pytest.fixture(scope="session")
def db_config():
    """Test database configuration.

    Override the default configuration to use TEST_DATABASE_URL env var
    or default to localhost postgres database.

    Auto-discovery will exclude "examples" directory by default.
    Tests that need migrations will set up their own directories.
    """
    default_url = f"postgresql://{getpass.getuser()}@localhost/postgres"
    url = os.getenv("TEST_DATABASE_URL", default_url)
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
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    default_url = f"postgresql://{getpass.getuser()}@localhost/postgres"
    url = os.getenv("TEST_DATABASE_URL", default_url)
    # Create config with explicit migrations_dirs
    config = DatabaseConfig(
        url=url,
        migrations_dirs=[str(migrations_dir)],
    )

    return SchemaManager(
        pool=isolated_db,
        config=config,
    )
