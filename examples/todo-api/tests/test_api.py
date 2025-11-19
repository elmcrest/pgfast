"""API endpoint tests showcasing pgfast's test_client fixture.

These tests demonstrate how to test FastAPI endpoints with isolated databases.
The api_client fixture (built on pgfast's test_client) automatically:
- Creates an isolated test database for each test
- Overrides the get_db_pool dependency to use the test database
- Provides an httpx AsyncClient for making HTTP requests

This is the recommended pattern for FastAPI integration testing with pgfast.
"""


class TestTodoEndpoints:
    """Test todo CRUD endpoints with api_client."""

    async def test_list_todos_empty(self, api_client):
        """Test listing todos when database is empty."""
        response = await api_client.get("/todos")
        assert response.status_code == 200
        assert response.json() == []

    async def test_create_and_get_todo(self, api_client, fixture_loader):
        """Test creating a todo and retrieving it."""
        # Load users fixture so we have a valid user_id
        await fixture_loader(["create_users"])

        # Create a todo
        create_response = await api_client.post(
            "/todos",
            json={"user_id": 1, "title": "Write API tests"},
        )
        assert create_response.status_code == 201

        todo = create_response.json()
        assert todo["title"] == "Write API tests"
        assert todo["user_id"] == 1
        assert todo["completed"] is False
        assert "id" in todo
        assert "created_at" in todo

        # Get the todo
        todo_id = todo["id"]
        get_response = await api_client.get(f"/todos/{todo_id}")
        assert get_response.status_code == 200
        assert get_response.json() == todo

    async def test_create_todo_with_invalid_user(self, api_client):
        """Test creating todo with non-existent user fails."""
        response = await api_client.post(
            "/todos",
            json={"user_id": 999, "title": "Should fail"},
        )
        assert response.status_code == 400
        assert "User not found" in response.json()["detail"]

    async def test_get_nonexistent_todo(self, api_client):
        """Test getting a todo that doesn't exist."""
        response = await api_client.get("/todos/999")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_update_todo_completion(self, api_client, fixture_loader):
        """Test updating a todo's completion status."""
        await fixture_loader(["create_users", "create_todos"])

        # Get an incomplete todo (fixture has ID 1 = "Buy groceries", incomplete)
        get_response = await api_client.get("/todos/1")
        assert get_response.status_code == 200
        original = get_response.json()
        assert original["completed"] is False

        # Mark it as completed
        update_response = await api_client.patch(
            "/todos/1",
            json={"completed": True},
        )
        assert update_response.status_code == 200

        updated = update_response.json()
        assert updated["id"] == 1
        assert updated["completed"] is True
        assert updated["updated_at"] > original["updated_at"]

    async def test_delete_todo(self, api_client, fixture_loader):
        """Test deleting a todo."""
        await fixture_loader(["create_users", "create_todos"])

        # Delete todo
        delete_response = await api_client.delete("/todos/1")
        assert delete_response.status_code == 204

        # Verify it's gone
        get_response = await api_client.get("/todos/1")
        assert get_response.status_code == 404

    async def test_delete_nonexistent_todo(self, api_client):
        """Test deleting a todo that doesn't exist."""
        response = await api_client.delete("/todos/999")
        assert response.status_code == 404

    async def test_filter_todos_by_user(self, api_client, fixture_loader):
        """Test filtering todos by user_id."""
        await fixture_loader(["create_users", "create_todos"])

        # Get Alice's todos (user_id=1, should have 3)
        response = await api_client.get("/todos?user_id=1")
        assert response.status_code == 200

        todos = response.json()
        assert len(todos) == 3
        assert all(todo["user_id"] == 1 for todo in todos)

        # Get Bob's todos (user_id=2, should have 2)
        response = await api_client.get("/todos?user_id=2")
        assert response.status_code == 200

        todos = response.json()
        assert len(todos) == 2
        assert all(todo["user_id"] == 2 for todo in todos)

    async def test_filter_todos_by_completion(self, api_client, fixture_loader):
        """Test filtering todos by completion status."""
        await fixture_loader(["create_users", "create_todos"])

        # Get completed todos (fixture has 2 completed)
        response = await api_client.get("/todos?completed=true")
        assert response.status_code == 200

        todos = response.json()
        assert len(todos) == 2
        assert all(todo["completed"] is True for todo in todos)

        # Get incomplete todos (fixture has 4 incomplete)
        response = await api_client.get("/todos?completed=false")
        assert response.status_code == 200

        todos = response.json()
        assert len(todos) == 4
        assert all(todo["completed"] is False for todo in todos)

    async def test_get_user_todos(self, api_client, fixture_loader):
        """Test getting todos for a specific user."""
        await fixture_loader(["create_users", "create_todos"])

        # Get todos for user 1 (Alice)
        response = await api_client.get("/users/1/todos")
        assert response.status_code == 200

        todos = response.json()
        assert len(todos) == 3
        assert all(todo["user_id"] == 1 for todo in todos)

        # Verify they're sorted by created_at DESC (most recent first)
        # Fixture creates them in order: 1, 2, 3
        # So reversed order should be: 3, 2, 1
        assert [todo["id"] for todo in todos] == [3, 2, 1]

    async def test_get_user_todos_nonexistent_user(self, api_client):
        """Test getting todos for a user that doesn't exist."""
        response = await api_client.get("/users/999/todos")
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]


class TestHealthCheck:
    """Test health check endpoint."""

    async def test_root_endpoint(self, api_client):
        """Test health check returns OK."""
        response = await api_client.get("/")
        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "message": "Todo API is running",
        }


class TestParallelAPITests:
    """Tests demonstrating parallel execution with API client.

    Each test gets its own isolated database and API client.
    Run with: pytest -n auto
    """

    async def test_parallel_api_1(self, api_client, fixture_loader):
        """Parallel API test 1."""
        await fixture_loader(["create_users"])
        response = await api_client.post(
            "/todos",
            json={"user_id": 1, "title": "Parallel task 1"},
        )
        assert response.status_code == 201

    async def test_parallel_api_2(self, api_client, fixture_loader):
        """Parallel API test 2."""
        await fixture_loader(["create_users"])
        response = await api_client.post(
            "/todos",
            json={"user_id": 2, "title": "Parallel task 2"},
        )
        assert response.status_code == 201

    async def test_parallel_api_3(self, api_client, fixture_loader):
        """Parallel API test 3."""
        await fixture_loader(["create_users"])
        response = await api_client.post(
            "/todos",
            json={"user_id": 3, "title": "Parallel task 3"},
        )
        assert response.status_code == 201


class TestEndToEndWorkflow:
    """End-to-end workflow test demonstrating a complete user journey."""

    async def test_complete_todo_workflow(self, api_client, fixture_loader):
        """Test a complete todo workflow from creation to deletion."""
        # Setup: Load users
        await fixture_loader(["create_users"])

        # 1. List todos (should be empty)
        response = await api_client.get("/todos")
        assert len(response.json()) == 0

        # 2. Create multiple todos
        todo_ids = []
        for i, title in enumerate(["Task 1", "Task 2", "Task 3"], 1):
            response = await api_client.post(
                "/todos",
                json={"user_id": 1, "title": title},
            )
            assert response.status_code == 201
            todo_ids.append(response.json()["id"])

        # 3. List todos (should have 3)
        response = await api_client.get("/todos")
        assert len(response.json()) == 3

        # 4. Complete one todo
        response = await api_client.patch(
            f"/todos/{todo_ids[0]}",
            json={"completed": True},
        )
        assert response.status_code == 200
        assert response.json()["completed"] is True

        # 5. Filter by completed
        response = await api_client.get("/todos?completed=true")
        completed = response.json()
        assert len(completed) == 1
        assert completed[0]["id"] == todo_ids[0]

        # 6. Filter by incomplete
        response = await api_client.get("/todos?completed=false")
        incomplete = response.json()
        assert len(incomplete) == 2
        assert all(t["id"] in todo_ids[1:] for t in incomplete)

        # 7. Delete a todo
        response = await api_client.delete(f"/todos/{todo_ids[1]}")
        assert response.status_code == 204

        # 8. Verify deletion
        response = await api_client.get("/todos")
        remaining = response.json()
        assert len(remaining) == 2
        assert all(t["id"] != todo_ids[1] for t in remaining)

        # 9. Get user's todos
        response = await api_client.get("/users/1/todos")
        user_todos = response.json()
        assert len(user_todos) == 2
        assert all(t["user_id"] == 1 for t in user_todos)
