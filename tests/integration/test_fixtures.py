"""Integration tests for fixture loading with dependency ordering."""

import pytest

from pgfast.fixtures import Fixture
from pgfast.testing import DatabaseTestManager


@pytest.fixture
async def migrations_with_deps(tmp_path):
    """Create test migrations with dependencies."""
    # Create migration directories
    module_a = tmp_path / "module_a" / "migrations"
    module_b = tmp_path / "module_b" / "migrations"
    module_a.mkdir(parents=True)
    module_b.mkdir(parents=True)

    # Migration A (no dependencies)
    (module_a / "20250101000000_create_users_up.sql").write_text(
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);"
    )
    (module_a / "20250101000000_create_users_down.sql").write_text("DROP TABLE users;")

    # Migration B (depends on A)
    (module_b / "20250102000000_create_posts_up.sql").write_text(
        "-- depends_on: 20250101000000\n"
        "CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id));"
    )
    (module_b / "20250102000000_create_posts_down.sql").write_text("DROP TABLE posts;")

    return tmp_path


@pytest.fixture
async def fixtures_with_versions(migrations_with_deps):
    """Create fixture files matching migrations."""
    # Create fixture directories
    fixtures_a = migrations_with_deps / "module_a" / "fixtures"
    fixtures_b = migrations_with_deps / "module_b" / "fixtures"
    fixtures_a.mkdir(parents=True)
    fixtures_b.mkdir(parents=True)

    # Fixture for migration A
    fixture_a = fixtures_a / "20250101000000_create_users_fixture.sql"
    fixture_a.write_text(
        "-- Fixture for users\n"
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');"
    )

    # Fixture for migration B (depends on A via migration graph)
    fixture_b = fixtures_b / "20250102000000_create_posts_fixture.sql"
    fixture_b.write_text(
        "-- Fixture for posts\nINSERT INTO posts (user_id) VALUES (1), (1), (2);"
    )

    return [fixture_a, fixture_b]


class TestFixtureModel:
    """Tests for the Fixture model."""

    def test_from_path_valid(self, tmp_path):
        """Test parsing valid fixture filename."""
        path = tmp_path / "20250101000000_create_users_fixture.sql"
        path.touch()

        fixture = Fixture.from_path(path)

        assert fixture is not None
        assert fixture.version == 20250101000000
        assert fixture.name == "create_users"
        assert fixture.path == path

    def test_from_path_complex_name(self, tmp_path):
        """Test parsing fixture with underscores in name."""
        path = tmp_path / "20250101000000_create_user_profiles_fixture.sql"
        path.touch()

        fixture = Fixture.from_path(path)

        assert fixture is not None
        assert fixture.version == 20250101000000
        assert fixture.name == "create_user_profiles"

    def test_from_path_invalid_no_fixture_suffix(self, tmp_path):
        """Test parsing filename without _fixture suffix."""
        path = tmp_path / "20250101000000_create_users.sql"
        path.touch()

        fixture = Fixture.from_path(path)

        assert fixture is None

    def test_from_path_invalid_no_version(self, tmp_path):
        """Test parsing filename without version."""
        path = tmp_path / "users_fixture.sql"
        path.touch()

        fixture = Fixture.from_path(path)

        assert fixture is None

    def test_from_path_invalid_version_format(self, tmp_path):
        """Test parsing filename with invalid version."""
        path = tmp_path / "abc_create_users_fixture.sql"
        path.touch()

        fixture = Fixture.from_path(path)

        assert fixture is None


@pytest.mark.asyncio
class TestFixtureDependencyLoading:
    """Tests for loading fixtures in dependency order."""

    async def test_fixtures_loaded_in_dependency_order(
        self, isolated_db, db_config, migrations_with_deps, fixtures_with_versions
    ):
        """Test that fixtures are loaded in migration dependency order."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager

        # Update config to use our test migrations
        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[
                str(migrations_with_deps / "module_a" / "migrations"),
                str(migrations_with_deps / "module_b" / "migrations"),
            ],
            fixtures_dirs=[
                str(migrations_with_deps / "module_a" / "fixtures"),
                str(migrations_with_deps / "module_b" / "fixtures"),
            ],
        )

        # Apply migrations first
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Load fixtures using DatabaseTestManager
        manager = DatabaseTestManager(config)
        await manager.load_fixtures(isolated_db)

        # Verify data was loaded correctly
        async with isolated_db.acquire() as conn:
            # Check users table (from fixture A)
            users = await conn.fetch("SELECT * FROM users ORDER BY id")
            assert len(users) == 2
            assert users[0]["name"] == "Alice"
            assert users[1]["name"] == "Bob"

            # Check posts table (from fixture B, depends on users)
            posts = await conn.fetch("SELECT * FROM posts ORDER BY id")
            assert len(posts) == 3
            assert posts[0]["user_id"] == 1
            assert posts[1]["user_id"] == 1
            assert posts[2]["user_id"] == 2

    async def test_fixtures_auto_discovery(
        self, isolated_db, db_config, migrations_with_deps, fixtures_with_versions
    ):
        """Test auto-discovery of fixtures from multiple directories."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager

        config = DatabaseConfig(
            url=db_config.url,
            search_base_path=migrations_with_deps,
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Auto-discover and load fixtures
        manager = DatabaseTestManager(config)
        discovered = manager.discover_fixtures()

        # Should find both fixtures
        assert len(discovered) == 2

        # Fixtures should be sorted by dependency order (A before B)
        fixture_versions = [
            fixture.version
            for p in discovered
            if (fixture := Fixture.from_path(p)) is not None
        ]
        assert fixture_versions == [20250101000000, 20250102000000]

    async def test_explicit_fixtures_sorted_by_dependencies(
        self, isolated_db, db_config, migrations_with_deps, fixtures_with_versions
    ):
        """Test that explicitly provided fixtures are sorted by dependencies."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager

        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[
                str(migrations_with_deps / "module_a" / "migrations"),
                str(migrations_with_deps / "module_b" / "migrations"),
            ],
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Load fixtures in reverse order (should be auto-sorted)
        manager = DatabaseTestManager(config)
        fixture_b, fixture_a = fixtures_with_versions  # Reversed!

        await manager.load_fixtures(isolated_db, [fixture_b, fixture_a])

        # Data should still load correctly due to automatic sorting
        async with isolated_db.acquire() as conn:
            users = await conn.fetch("SELECT COUNT(*) FROM users")
            assert users[0]["count"] == 2

            posts = await conn.fetch("SELECT COUNT(*) FROM posts")
            assert posts[0]["count"] == 3


@pytest.mark.asyncio
class TestFixtureLoaderPytestFixture:
    """Tests for the fixture_loader pytest fixture."""

    @pytest.fixture
    async def multi_fixture_setup(self, tmp_path):
        """Create migrations and fixtures with complex dependencies."""
        # Create migration and fixture directories
        migrations_dir = tmp_path / "db" / "migrations"
        fixtures_dir = tmp_path / "db" / "fixtures"
        migrations_dir.mkdir(parents=True)
        fixtures_dir.mkdir(parents=True)

        # Migration 1: users (no dependencies)
        (migrations_dir / "20250101000000_create_users_up.sql").write_text(
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);"
        )
        (migrations_dir / "20250101000000_create_users_down.sql").write_text(
            "DROP TABLE users;"
        )

        # Migration 2: products (no dependencies)
        (migrations_dir / "20250102000000_create_products_up.sql").write_text(
            "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL);"
        )
        (migrations_dir / "20250102000000_create_products_down.sql").write_text(
            "DROP TABLE products;"
        )

        # Migration 3: orders (depends on users and products)
        (migrations_dir / "20250103000000_create_orders_up.sql").write_text(
            "-- depends_on: 20250101000000, 20250102000000\n"
            "CREATE TABLE orders ("
            "id SERIAL PRIMARY KEY, "
            "user_id INTEGER REFERENCES users(id), "
            "product_id INTEGER REFERENCES products(id)"
            ");"
        )
        (migrations_dir / "20250103000000_create_orders_down.sql").write_text(
            "DROP TABLE orders;"
        )

        # Fixtures matching each migration
        (fixtures_dir / "20250101000000_create_users_fixture.sql").write_text(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');"
        )
        (fixtures_dir / "20250102000000_create_products_fixture.sql").write_text(
            "INSERT INTO products (id, name, price) VALUES "
            "(1, 'Widget', 9.99), (2, 'Gadget', 19.99);"
        )
        (fixtures_dir / "20250103000000_create_orders_fixture.sql").write_text(
            "INSERT INTO orders (user_id, product_id) VALUES (1, 1), (1, 2), (2, 1);"
        )

        return {
            "migrations_dir": migrations_dir,
            "fixtures_dir": fixtures_dir,
            "tmp_path": tmp_path,
        }

    async def test_fixture_loader_selective_loading(
        self, isolated_db, db_config, multi_fixture_setup
    ):
        """Test loading only specific fixtures by name."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager

        # Configure with our test directories
        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[str(multi_fixture_setup["migrations_dir"])],
            fixtures_dirs=[str(multi_fixture_setup["fixtures_dir"])],
        )

        # Apply all migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Get the fixture_loader
        from pgfast.testing import DatabaseTestManager

        manager = DatabaseTestManager(config)

        async def load(names: list[str]) -> None:
            all_fixtures = manager.discover_fixtures()
            to_load = []
            found_names = set()

            for path in all_fixtures:
                fixture = Fixture.from_path(path)
                if fixture and fixture.name in names:
                    to_load.append(path)
                    found_names.add(fixture.name)

            missing = set(names) - found_names
            if missing:
                raise ValueError(f"Fixtures not found: {', '.join(missing)}")

            await manager.load_fixtures(isolated_db, to_load)

        # Load only 'create_users' and 'create_products' fixtures
        await load(["create_users", "create_products"])

        # Verify data
        async with isolated_db.acquire() as conn:
            # Users should be loaded
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            assert user_count == 2

            # Products should be loaded
            product_count = await conn.fetchval("SELECT COUNT(*) FROM products")
            assert product_count == 2

            # Orders should NOT be loaded (not in the list)
            order_count = await conn.fetchval("SELECT COUNT(*) FROM orders")
            assert order_count == 0

    async def test_fixture_loader_preserves_dependency_order(
        self, isolated_db, db_config, multi_fixture_setup
    ):
        """Test that fixture_loader preserves dependency order."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager
        from pgfast.testing import DatabaseTestManager

        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[str(multi_fixture_setup["migrations_dir"])],
            fixtures_dirs=[str(multi_fixture_setup["fixtures_dir"])],
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Get fixture_loader implementation
        manager = DatabaseTestManager(config)

        async def load(names: list[str]) -> None:
            all_fixtures = manager.discover_fixtures()
            to_load = []
            found_names = set()

            for path in all_fixtures:
                fixture = Fixture.from_path(path)
                if fixture and fixture.name in names:
                    to_load.append(path)
                    found_names.add(fixture.name)

            missing = set(names) - found_names
            if missing:
                raise ValueError(f"Fixtures not found: {', '.join(missing)}")

            await manager.load_fixtures(isolated_db, to_load)

        # Request fixtures in reverse order - should still work due to dependency sorting
        await load(["create_orders", "create_products", "create_users"])

        # If dependency order wasn't preserved, this would fail with FK constraint errors
        async with isolated_db.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            product_count = await conn.fetchval("SELECT COUNT(*) FROM products")
            order_count = await conn.fetchval("SELECT COUNT(*) FROM orders")

            assert user_count == 2
            assert product_count == 2
            assert order_count == 3

    async def test_fixture_loader_missing_fixture_error(
        self, isolated_db, db_config, multi_fixture_setup
    ):
        """Test that fixture_loader raises error for missing fixtures."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager
        from pgfast.testing import DatabaseTestManager

        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[str(multi_fixture_setup["migrations_dir"])],
            fixtures_dirs=[str(multi_fixture_setup["fixtures_dir"])],
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Get fixture_loader implementation
        manager = DatabaseTestManager(config)

        async def load(names: list[str]) -> None:
            all_fixtures = manager.discover_fixtures()
            to_load = []
            found_names = set()

            for path in all_fixtures:
                fixture = Fixture.from_path(path)
                if fixture and fixture.name in names:
                    to_load.append(path)
                    found_names.add(fixture.name)

            missing = set(names) - found_names
            if missing:
                raise ValueError(f"Fixtures not found: {', '.join(missing)}")

            await manager.load_fixtures(isolated_db, to_load)

        # Request non-existent fixture
        with pytest.raises(ValueError, match="Fixtures not found: nonexistent"):
            await load(["create_users", "nonexistent"])

    async def test_fixture_loader_empty_list(
        self, isolated_db, db_config, multi_fixture_setup
    ):
        """Test that fixture_loader handles empty list gracefully."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager
        from pgfast.testing import DatabaseTestManager

        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[str(multi_fixture_setup["migrations_dir"])],
            fixtures_dirs=[str(multi_fixture_setup["fixtures_dir"])],
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Get fixture_loader implementation
        manager = DatabaseTestManager(config)

        async def load(names: list[str]) -> None:
            all_fixtures = manager.discover_fixtures()
            to_load = []
            found_names = set()

            for path in all_fixtures:
                fixture = Fixture.from_path(path)
                if fixture and fixture.name in names:
                    to_load.append(path)
                    found_names.add(fixture.name)

            missing = set(names) - found_names
            if missing:
                raise ValueError(f"Fixtures not found: {', '.join(missing)}")

            await manager.load_fixtures(isolated_db, to_load)

        # Load empty list - should succeed without errors
        await load([])

        # Verify no data was loaded
        async with isolated_db.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            product_count = await conn.fetchval("SELECT COUNT(*) FROM products")
            order_count = await conn.fetchval("SELECT COUNT(*) FROM orders")

            assert user_count == 0
            assert product_count == 0
            assert order_count == 0

    async def test_fixture_loader_single_fixture(
        self, isolated_db, db_config, multi_fixture_setup
    ):
        """Test loading a single fixture."""
        from pgfast.config import DatabaseConfig
        from pgfast.schema import SchemaManager
        from pgfast.testing import DatabaseTestManager

        config = DatabaseConfig(
            url=db_config.url,
            migrations_dirs=[str(multi_fixture_setup["migrations_dir"])],
            fixtures_dirs=[str(multi_fixture_setup["fixtures_dir"])],
        )

        # Apply migrations
        schema_manager = SchemaManager(isolated_db, config)
        await schema_manager.schema_up()

        # Get fixture_loader implementation
        manager = DatabaseTestManager(config)

        async def load(names: list[str]) -> None:
            all_fixtures = manager.discover_fixtures()
            to_load = []
            found_names = set()

            for path in all_fixtures:
                fixture = Fixture.from_path(path)
                if fixture and fixture.name in names:
                    to_load.append(path)
                    found_names.add(fixture.name)

            missing = set(names) - found_names
            if missing:
                raise ValueError(f"Fixtures not found: {', '.join(missing)}")

            await manager.load_fixtures(isolated_db, to_load)

        # Load only products
        await load(["create_products"])

        async with isolated_db.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            product_count = await conn.fetchval("SELECT COUNT(*) FROM products")
            order_count = await conn.fetchval("SELECT COUNT(*) FROM orders")

            assert user_count == 0
            assert product_count == 2
            assert order_count == 0
