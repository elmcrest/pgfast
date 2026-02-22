"""Unit tests for pgfast.pytest fixture behavior."""

import pytest

import pgfast.pytest as pgfast_pytest
from pgfast.config import DatabaseConfig


def _fixture_func(fixture_fn):
    """Get underlying callable for direct unit-testing of fixture logic."""
    return getattr(fixture_fn, "__wrapped__", fixture_fn)


def test_db_config_uses_test_fragment_prefix(monkeypatch):
    """db_config should load TEST_POSTGRES_* fragments via DatabaseConfig.from_env."""
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    monkeypatch.setenv("TEST_POSTGRES_HOST", "dbhost")
    monkeypatch.setenv("TEST_POSTGRES_PORT", "5434")
    monkeypatch.setenv("TEST_POSTGRES_USER", "tester")
    monkeypatch.setenv("TEST_POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("TEST_POSTGRES_DB", "testdb")

    config = _fixture_func(pgfast_pytest.db_config)()

    assert config.url == "postgresql://tester:secret@dbhost:5434/testdb"
    assert config.min_connections == 2
    assert config.max_connections == 5


@pytest.mark.asyncio
async def test_template_db_detects_python_only_migrations(tmp_path, monkeypatch):
    """template_db should create template when only Python migrations exist."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "20250101000000_py_only_up.py").write_text(
        "async def migrate(conn):\n    pass\n"
    )
    (migrations_dir / "20250101000000_py_only_down.py").write_text(
        "async def migrate(conn):\n    pass\n"
    )

    config = DatabaseConfig(
        url="postgresql://localhost/postgres",
        migrations_dirs=[str(migrations_dir)],
    )

    created: list[str] = []
    destroyed: list[str] = []

    class StubManager:
        def __init__(self, _config):
            pass

        async def create_template_db(self, template_name: str) -> None:
            created.append(template_name)

        async def destroy_template_db(self, template_name: str) -> None:
            destroyed.append(template_name)

    monkeypatch.setattr(pgfast_pytest, "DatabaseTestManager", StubManager)

    template_fixture = _fixture_func(pgfast_pytest.template_db)
    agen = template_fixture(config)

    template_name = await agen.__anext__()
    assert template_name.startswith("pgfast_template_")
    assert created == [template_name]

    with pytest.raises(StopAsyncIteration):
        await agen.__anext__()

    assert destroyed == [template_name]
