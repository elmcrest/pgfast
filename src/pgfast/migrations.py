import hashlib
import re
from pathlib import Path

from pydantic import BaseModel


class Migration(BaseModel):
    """Represents a database migration.

    Migrations can declare dependencies on other migrations using a comment
    header in the up/down files:
    -- depends_on: 20240101000000, 20240102000000

    Checksums are calculated from the combined contents of both up and down files
    to detect if migrations have been modified after being applied.
    """

    version: int
    name: str
    up_file: Path
    down_file: Path
    source_dir: Path  # Track which directory this migration came from

    @property
    def is_complete(self) -> bool:
        """Check if both up and down files exist."""
        return self.up_file.exists() and self.down_file.exists()

    @property
    def dependencies(self) -> list[int]:
        """Parse and return list of migration dependencies.

        Scans both up and down files for dependency declarations in the format:
        -- depends_on: 20240101000000, 20240102000000

        Returns:
            List of migration version numbers this migration depends on
        """
        deps = set()

        for file_path in [self.up_file, self.down_file]:
            if not file_path.exists():
                continue

            content = file_path.read_text()

            # Find all dependency declarations
            # Pattern: -- depends_on: version1, version2, ...
            pattern = r"--\s*depends_on:\s*([\d,\s]+)"
            matches = re.finditer(pattern, content, re.IGNORECASE)

            for match in matches:
                # Extract and parse version numbers
                versions_str = match.group(1)
                for version_str in versions_str.split(","):
                    version_str = version_str.strip()
                    if version_str:
                        try:
                            deps.add(int(version_str))
                        except ValueError:
                            # Skip invalid version numbers
                            pass

        return sorted(list(deps))

    def calculate_checksum(self) -> str:
        """Calculate SHA-256 checksum of migration files.

        The checksum is calculated from the combined contents of both the up
        and down migration files to detect any modifications.

        Returns:
            Hexadecimal string representation of the SHA-256 checksum

        Raises:
            FileNotFoundError: If migration files don't exist
        """
        hasher = hashlib.sha256()

        # Read and hash up file
        if self.up_file.exists():
            hasher.update(self.up_file.read_bytes())

        # Read and hash down file
        if self.down_file.exists():
            hasher.update(self.down_file.read_bytes())

        return hasher.hexdigest()

    def read_sql(self, direction: str = "up") -> str:
        """Read SQL content from migration file.

        Args:
            direction: Either "up" or "down"

        Returns:
            SQL content as string

        Raises:
            ValueError: If direction is not "up" or "down"
            FileNotFoundError: If the file doesn't exist
        """
        if direction == "up":
            return self.up_file.read_text()
        elif direction == "down":
            return self.down_file.read_text()
        else:
            raise ValueError(f"Invalid direction: {direction}")
