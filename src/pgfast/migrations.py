from pathlib import Path

from pydantic import BaseModel


class Migration(BaseModel):
    """Represents a database migration."""

    version: int
    name: str
    up_file: Path
    down_file: Path

    @property
    def is_complete(self) -> bool:
        """Check if both up and down files exist."""
        return self.up_file.exists() and self.down_file.exists()
