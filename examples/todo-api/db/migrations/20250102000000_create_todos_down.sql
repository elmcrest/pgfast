-- Migration: create_todos (rollback)
-- Drop todos table and trigger

DROP TRIGGER IF EXISTS update_todos_updated_at ON todos;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS todos;
