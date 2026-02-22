"""Unit tests for public package exports."""

from pgfast import ChecksumError, DependencyError


def test_public_api_exports_dependency_and_checksum_errors():
    """Package root should export dependency/checksum exception types."""
    assert issubclass(DependencyError, Exception)
    assert issubclass(ChecksumError, Exception)
