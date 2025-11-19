"""Simple Todo API showcasing pgfast features.

This example demonstrates:
- FastAPI integration with lifespan and dependency injection
- Raw SQL queries (no ORM)
- Connection pooling with asyncpg
- Clean separation of concerns
"""

import os
from datetime import datetime

import asyncpg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from pgfast import DatabaseConfig, create_lifespan, get_db_pool

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/todo_api")
config = DatabaseConfig(url=DATABASE_URL, min_connections=2, max_connections=10)

# FastAPI app with pgfast lifespan integration
app = FastAPI(
    title="Todo API",
    description="Simple todo CRUD API built with FastAPI and pgfast",
    lifespan=create_lifespan(config),
)


# Pydantic models
class TodoCreate(BaseModel):
    user_id: int
    title: str


class TodoUpdate(BaseModel):
    completed: bool


class Todo(BaseModel):
    id: int
    user_id: int
    title: str
    completed: bool
    created_at: datetime
    updated_at: datetime


# Routes
@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "message": "Todo API is running"}


@app.get("/todos", response_model=list[Todo])
async def list_todos(
    user_id: int | None = None,
    completed: bool | None = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """List todos with optional filters.

    Query parameters:
    - user_id: Filter by user ID
    - completed: Filter by completion status
    """
    async with pool.acquire() as conn:
        # Build query dynamically based on filters
        query = "SELECT * FROM todos WHERE 1=1"
        params = []

        if user_id is not None:
            params.append(user_id)
            query += f" AND user_id = ${len(params)}"

        if completed is not None:
            params.append(completed)
            query += f" AND completed = ${len(params)}"

        query += " ORDER BY created_at DESC"

        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@app.get("/todos/{todo_id}", response_model=Todo)
async def get_todo(todo_id: int, pool: asyncpg.Pool = Depends(get_db_pool)):
    """Get a specific todo by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM todos WHERE id = $1",
            todo_id,
        )

        if not row:
            raise HTTPException(status_code=404, detail="Todo not found")

        return dict(row)


@app.post("/todos", response_model=Todo, status_code=201)
async def create_todo(todo: TodoCreate, pool: asyncpg.Pool = Depends(get_db_pool)):
    """Create a new todo."""
    async with pool.acquire() as conn:
        # Verify user exists
        user_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
            todo.user_id,
        )

        if not user_exists:
            raise HTTPException(status_code=400, detail="User not found")

        # Insert todo
        row = await conn.fetchrow(
            """
            INSERT INTO todos (user_id, title)
            VALUES ($1, $2)
            RETURNING *
            """,
            todo.user_id,
            todo.title,
        )

        return dict(row)


@app.patch("/todos/{todo_id}", response_model=Todo)
async def update_todo(
    todo_id: int,
    todo: TodoUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """Update todo completion status."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE todos
            SET completed = $1
            WHERE id = $2
            RETURNING *
            """,
            todo.completed,
            todo_id,
        )

        if not row:
            raise HTTPException(status_code=404, detail="Todo not found")

        return dict(row)


@app.delete("/todos/{todo_id}", status_code=204)
async def delete_todo(todo_id: int, pool: asyncpg.Pool = Depends(get_db_pool)):
    """Delete a todo."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM todos WHERE id = $1",
            todo_id,
        )

        # Check if any row was deleted
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Todo not found")


@app.get("/users/{user_id}/todos", response_model=list[Todo])
async def get_user_todos(user_id: int, pool: asyncpg.Pool = Depends(get_db_pool)):
    """Get all todos for a specific user."""
    async with pool.acquire() as conn:
        # Verify user exists
        user_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
            user_id,
        )

        if not user_exists:
            raise HTTPException(status_code=404, detail="User not found")

        rows = await conn.fetch(
            "SELECT * FROM todos WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )

        return [dict(row) for row in rows]
