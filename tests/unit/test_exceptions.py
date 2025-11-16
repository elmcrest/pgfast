from pgfast.exceptions import (
    PgfastError,
    ConnectionError,
    SchemaError,
    ConfigurationError,
)


def test_base_exception_hierarchy():
    """PgfastError should be base for all pgfast exceptions."""
    assert issubclass(ConnectionError, PgfastError)
    assert issubclass(SchemaError, PgfastError)
    assert issubclass(ConfigurationError, PgfastError)


def test_exceptions_include_message():
    """All exceptions should support error messages."""
    error = PgfastError("test error")
    assert str(error) == "test error"

    conn_error = ConnectionError("connection failed")
    assert "connection failed" in str(conn_error)


def test_exceptions_can_wrap_cause():
    """Exceptions should preserve original cause."""
    original = ValueError("original error")

    try:
        raise SchemaError("schema failed") from original
    except SchemaError as wrapped:
        assert wrapped.__cause__ == original
        assert isinstance(wrapped.__cause__, ValueError)
        assert str(wrapped.__cause__) == "original error"
