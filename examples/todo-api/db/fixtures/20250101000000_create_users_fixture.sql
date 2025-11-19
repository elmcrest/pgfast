-- Test fixture: users
-- Sample users for testing

INSERT INTO users (id, username, email, created_at) VALUES
(1, 'alice', 'alice@example.com', '2025-01-01 10:00:00'),
(2, 'bob', 'bob@example.com', '2025-01-01 11:00:00'),
(3, 'charlie', 'charlie@example.com', '2025-01-01 12:00:00');

-- Reset sequence to continue from last inserted ID
SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));
