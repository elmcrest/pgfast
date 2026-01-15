"""Unit tests for CLI functionality."""

from pathlib import Path

from pgfast.cli import create_parser


class TestSchemaUpParser:
    """Tests for 'pgfast schema up' argument parsing."""

    def test_timeout_flag_default_is_none(self):
        """--timeout should default to None (no limit)."""
        parser = create_parser()
        args = parser.parse_args(["schema", "up"])
        assert args.timeout is None

    def test_timeout_flag_accepts_float(self):
        """--timeout should accept a float value."""
        parser = create_parser()
        args = parser.parse_args(["schema", "up", "--timeout", "300.5"])
        assert args.timeout == 300.5

    def test_timeout_flag_accepts_integer_as_float(self):
        """--timeout should accept an integer and convert to float."""
        parser = create_parser()
        args = parser.parse_args(["schema", "up", "--timeout", "60"])
        assert args.timeout == 60.0


class TestSchemaDownParser:
    """Tests for 'pgfast schema down' argument parsing."""

    def test_timeout_flag_default_is_none(self):
        """--timeout should default to None (no limit)."""
        parser = create_parser()
        args = parser.parse_args(["schema", "down"])
        assert args.timeout is None

    def test_timeout_flag_accepts_float(self):
        """--timeout should accept a float value."""
        parser = create_parser()
        args = parser.parse_args(["schema", "down", "--timeout", "600"])
        assert args.timeout == 600.0


class TestSchemaCreateTargetDir:
    """Tests for migration target directory resolution."""

    def _resolve_target_dir(self, module: str) -> Path:
        """Replicate the target directory logic from cmd_schema_create."""
        module_path = Path(module)
        if "migrations" in module_path.parts:
            return module_path
        else:
            return module_path / "migrations"

    def test_module_without_migrations_appends_migrations(self):
        """Module path without 'migrations' should get /migrations appended."""
        assert self._resolve_target_dir("users") == Path("users/migrations")
        assert self._resolve_target_dir("app/users") == Path("app/users/migrations")
        assert self._resolve_target_dir("db") == Path("db/migrations")

    def test_path_with_migrations_used_as_is(self):
        """Path containing 'migrations' should be used as-is."""
        assert self._resolve_target_dir("migrations") == Path("migrations")
        assert self._resolve_target_dir("migrations/users") == Path("migrations/users")
        assert self._resolve_target_dir("db/migrations") == Path("db/migrations")
        assert self._resolve_target_dir("db/migrations/users") == Path(
            "db/migrations/users"
        )

    def test_migrations_at_any_level_detected(self):
        """'migrations' at any level in the path should be detected."""
        assert self._resolve_target_dir("a/migrations/b/c") == Path("a/migrations/b/c")
        assert self._resolve_target_dir("migrations/a/b") == Path("migrations/a/b")

    def test_partial_match_not_detected(self):
        """Partial matches like 'my_migrations' should not be detected."""
        # 'my_migrations' is a single path component, not 'migrations'
        assert self._resolve_target_dir("my_migrations") == Path(
            "my_migrations/migrations"
        )
        assert self._resolve_target_dir("migrations_old") == Path(
            "migrations_old/migrations"
        )
