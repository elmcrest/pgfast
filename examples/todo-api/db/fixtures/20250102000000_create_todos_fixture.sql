-- Test fixture: todos
-- Sample todos for testing

INSERT INTO todos (id, user_id, title, completed, created_at) VALUES
-- Alice's todos
(1, 1, 'Buy groceries', false, '2025-01-01 10:30:00'),
(2, 1, 'Write documentation', true, '2025-01-01 10:31:00'),
(3, 1, 'Review pull request', false, '2025-01-01 10:32:00'),

-- Bob's todos
(4, 2, 'Deploy to production', true, '2025-01-01 11:30:00'),
(5, 2, 'Fix bug in auth', false, '2025-01-01 11:31:00'),

-- Charlie's todos
(6, 3, 'Refactor database layer', false, '2025-01-01 12:30:00');

-- Reset sequence to continue from last inserted ID
SELECT setval('todos_id_seq', (SELECT MAX(id) FROM todos));
