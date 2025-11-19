"""Tests for todo API showcasing pgfast features.

Key pgfast features demonstrated:
1. isolated_db: Each test gets a fresh database (fast template cloning)
2. fixture_loader: Load only the fixtures you need
3. db_with_fixtures: Load all fixtures automatically
4. Parallel test execution: All tests run concurrently without conflicts
"""

import pytest


class TestTodosWithIsolatedDB:
    """Tests using isolated_db - clean database per test, no fixtures."""

    async def test_create_user_and_todo(self, isolated_db):
        """Test creating a user and todo from scratch.

        Demonstrates: isolated_db gives you a clean database with schema applied.
        """
        async with isolated_db.acquire() as conn:
            # Create a user
            user = await conn.fetchrow(
                """
                INSERT INTO users (username, email)
                VALUES ($1, $2)
                RETURNING id, username, email
                """,
                "testuser",
                "test@example.com",
            )

            assert user["username"] == "testuser"
            assert user["email"] == "test@example.com"

            # Create a todo for the user
            todo = await conn.fetchrow(
                """
                INSERT INTO todos (user_id, title, completed)
                VALUES ($1, $2, $3)
                RETURNING id, user_id, title, completed
                """,
                user["id"],
                "Test todo",
                False,
            )

            assert todo["user_id"] == user["id"]
            assert todo["title"] == "Test todo"
            assert todo["completed"] is False

    async def test_foreign_key_constraint(self, isolated_db):
        """Test that foreign key constraints are enforced.

        Demonstrates: Migrations are properly applied with all constraints.
        """
        async with isolated_db.acquire() as conn:
            # Try to create todo with non-existent user_id
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    """
                    INSERT INTO todos (user_id, title)
                    VALUES ($1, $2)
                    """,
                    999,  # Non-existent user
                    "Invalid todo",
                )

            assert "foreign key constraint" in str(exc_info.value).lower()

    async def test_updated_at_trigger(self, isolated_db):
        """Test that updated_at trigger works correctly.

        Demonstrates: Complex migrations with triggers work correctly.
        """
        async with isolated_db.acquire() as conn:
            # Create user and todo
            user_id = await conn.fetchval(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id",
                "alice",
                "alice@example.com",
            )

            todo_id = await conn.fetchval(
                "INSERT INTO todos (user_id, title) VALUES ($1, $2) RETURNING id",
                user_id,
                "Test todo",
            )

            # Get initial timestamps
            initial = await conn.fetchrow(
                "SELECT created_at, updated_at FROM todos WHERE id = $1", todo_id
            )

            # Update the todo
            await conn.execute(
                "UPDATE todos SET completed = true WHERE id = $1", todo_id
            )

            # Check updated_at changed
            updated = await conn.fetchrow(
                "SELECT created_at, updated_at FROM todos WHERE id = $1", todo_id
            )

            assert updated["created_at"] == initial["created_at"]
            assert updated["updated_at"] > initial["updated_at"]


class TestTodosWithSelectiveFixtures:
    """Tests using fixture_loader - load only what you need.

    Demonstrates: fixture_loader allows loading specific fixtures by name,
    maintaining dependency order automatically.
    """

    async def test_load_only_users(self, isolated_db, fixture_loader):
        """Load only users fixture, not todos.

        Demonstrates: Selective fixture loading for faster tests.
        """
        # Load only the users fixture
        await fixture_loader(["create_users"])

        async with isolated_db.acquire() as conn:
            # Users should be loaded
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            assert user_count == 3

            # Todos should be empty
            todo_count = await conn.fetchval("SELECT COUNT(*) FROM todos")
            assert todo_count == 0

            # Verify specific user
            alice = await conn.fetchrow(
                "SELECT * FROM users WHERE username = $1", "alice"
            )
            assert alice["email"] == "alice@example.com"

    async def test_load_users_and_todos(self, isolated_db, fixture_loader):
        """Load both users and todos fixtures.

        Demonstrates: Load multiple fixtures with automatic dependency ordering.
        """
        # Load both fixtures (order doesn't matter, dependencies are handled)
        await fixture_loader(["create_todos", "create_users"])

        async with isolated_db.acquire() as conn:
            # Both should be loaded
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            todo_count = await conn.fetchval("SELECT COUNT(*) FROM todos")

            assert user_count == 3
            assert todo_count == 6

            # Test FK relationship works
            alice_todos = await conn.fetch(
                """
                SELECT t.* FROM todos t
                JOIN users u ON t.user_id = u.id
                WHERE u.username = $1
                """,
                "alice",
            )

            assert len(alice_todos) == 3

    async def test_query_user_todos(self, isolated_db, fixture_loader):
        """Test querying todos for a specific user.

        Demonstrates: Testing with realistic data using selective fixtures.
        """
        await fixture_loader(["create_users", "create_todos"])

        async with isolated_db.acquire() as conn:
            # Get Bob's todos
            bob_todos = await conn.fetch(
                """
                SELECT t.* FROM todos t
                JOIN users u ON t.user_id = u.id
                WHERE u.username = $1
                ORDER BY t.created_at
                """,
                "bob",
            )

            assert len(bob_todos) == 2
            assert bob_todos[0]["title"] == "Deploy to production"
            assert bob_todos[0]["completed"] is True
            assert bob_todos[1]["title"] == "Fix bug in auth"
            assert bob_todos[1]["completed"] is False

    async def test_todo_counts_by_completion(self, isolated_db, fixture_loader):
        """Test aggregating todos by completion status.

        Demonstrates: Complex queries with test data.
        """
        await fixture_loader(["create_users", "create_todos"])

        async with isolated_db.acquire() as conn:
            result = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE completed = true) as completed,
                    COUNT(*) FILTER (WHERE completed = false) as incomplete,
                    COUNT(*) as total
                FROM todos
                """
            )

            assert result["completed"] == 2
            assert result["incomplete"] == 4
            assert result["total"] == 6


class TestTodosWithAllFixtures:
    """Tests using db_with_fixtures - all fixtures loaded automatically.

    Demonstrates: db_with_fixtures loads all fixtures in dependency order,
    great for tests that need the full dataset.
    """

    async def test_all_data_loaded(self, db_with_fixtures):
        """Verify all fixtures are loaded automatically."""
        async with db_with_fixtures.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            todo_count = await conn.fetchval("SELECT COUNT(*) FROM todos")

            assert user_count == 3
            assert todo_count == 6

    async def test_user_todo_relationships(self, db_with_fixtures):
        """Test relationships across all users.

        Demonstrates: Testing with full dataset already loaded.
        """
        async with db_with_fixtures.acquire() as conn:
            # Get todo count per user
            results = await conn.fetch(
                """
                SELECT u.username, COUNT(t.id) as todo_count
                FROM users u
                LEFT JOIN todos t ON u.id = t.user_id
                GROUP BY u.username
                ORDER BY u.username
                """
            )

            assert len(results) == 3
            assert results[0]["username"] == "alice"
            assert results[0]["todo_count"] == 3
            assert results[1]["username"] == "bob"
            assert results[1]["todo_count"] == 2
            assert results[2]["username"] == "charlie"
            assert results[2]["todo_count"] == 1

    async def test_completed_todos_across_users(self, db_with_fixtures):
        """Test completed todos across all users."""
        async with db_with_fixtures.acquire() as conn:
            completed = await conn.fetch(
                """
                SELECT u.username, t.title
                FROM todos t
                JOIN users u ON t.user_id = u.id
                WHERE t.completed = true
                ORDER BY u.username, t.title
                """
            )

            assert len(completed) == 2
            assert completed[0]["username"] == "alice"
            assert completed[0]["title"] == "Write documentation"
            assert completed[1]["username"] == "bob"
            assert completed[1]["title"] == "Deploy to production"


class TestCascadeDelete:
    """Test cascade delete behavior.

    Demonstrates: Testing database constraints and cascade behavior.
    """

    async def test_deleting_user_cascades_to_todos(self, isolated_db, fixture_loader):
        """Test that deleting a user deletes their todos."""
        await fixture_loader(["create_users", "create_todos"])

        async with isolated_db.acquire() as conn:
            # Verify Alice has todos
            alice_todo_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM todos t
                JOIN users u ON t.user_id = u.id
                WHERE u.username = $1
                """,
                "alice",
            )
            assert alice_todo_count == 3

            # Delete Alice
            await conn.execute("DELETE FROM users WHERE username = $1", "alice")

            # Verify her todos are gone (CASCADE)
            remaining_todos = await conn.fetchval("SELECT COUNT(*) FROM todos")
            assert remaining_todos == 3  # 6 - 3 = 3 (Bob's 2 + Charlie's 1)

            # Verify other users' todos are intact
            bob_todos = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM todos t
                JOIN users u ON t.user_id = u.id
                WHERE u.username = $1
                """,
                "bob",
            )
            assert bob_todos == 2


class TestParallelExecution:
    """These tests demonstrate parallel execution capability.

    All tests in this class (and the entire file) can run in parallel
    using pytest -n auto. Each test gets its own isolated database,
    preventing any conflicts.
    """

    async def test_concurrent_user_creation_1(self, isolated_db):
        """Parallel test 1: Create user in isolated database."""
        async with isolated_db.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (username, email) VALUES ($1, $2)",
                "parallel1",
                "parallel1@example.com",
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM users")
            assert count == 1

    async def test_concurrent_user_creation_2(self, isolated_db):
        """Parallel test 2: Create different user in separate database."""
        async with isolated_db.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (username, email) VALUES ($1, $2)",
                "parallel2",
                "parallel2@example.com",
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM users")
            assert count == 1  # Each test gets its own database!

    async def test_concurrent_with_fixtures_1(self, isolated_db, fixture_loader):
        """Parallel test 3: Load fixtures independently."""
        await fixture_loader(["create_users"])
        async with isolated_db.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM users")
            assert count == 3

    async def test_concurrent_with_fixtures_2(self, isolated_db, fixture_loader):
        """Parallel test 4: Load same fixtures in separate database."""
        await fixture_loader(["create_users", "create_todos"])
        async with isolated_db.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            todo_count = await conn.fetchval("SELECT COUNT(*) FROM todos")
            assert user_count == 3
            assert todo_count == 6
