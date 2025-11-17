"""Fixture models and utilities."""

from pathlib import Path

from pydantic import BaseModel


class Fixture(BaseModel):
    """Represents a database fixture.

    Fixtures follow the naming convention:
    {version}_{migration_name}_fixture.sql

    They inherit dependencies from their corresponding migration files.
    The version links the fixture to a migration in the dependency graph.
    """

    path: Path
    version: int
    name: str  # Migration name (without version or _fixture suffix)

    @classmethod
    def from_path(cls, path: Path) -> "Fixture | None":
        """Create Fixture from file path.

        Parses filename in format: {version}_{name}_fixture.sql

        Args:
            path: Path to fixture file

        Returns:
            Fixture instance or None if filename doesn't match expected format
        """
        # Expected format: {version}_{name}_fixture.sql
        stem = path.stem

        if not stem.endswith("_fixture"):
            return None

        # Remove _fixture suffix
        stem_without_suffix = stem[:-8]  # len("_fixture") == 8

        # Split by underscore
        parts = stem_without_suffix.split("_")

        if len(parts) < 2:
            return None

        version_str = parts[0]
        name = "_".join(parts[1:])

        try:
            version = int(version_str)
        except ValueError:
            return None

        return cls(path=path, version=version, name=name)
