# id_mapping.py
import json
from typing import Dict, Optional
from pathlib import Path


class IDMapper:
    """
    A utility class to manage ID mappings between source and destination tables
    during database migration.
    """

    def __init__(
        self,
        mapping_name: str,
        mapping_dir: str = "migration_mappings",
    ):
        """
        Initialize the ID mapper.

        Args:
            mapping_name: Name of the mapping (e.g., 'users', 'products')
            mapping_dir: Directory to store mapping files
        """
        self.mapping_name = mapping_name
        self.mapping_dir = Path(mapping_dir)
        self.mapping_file = self.mapping_dir / f"{mapping_name}_mapping.json"
        self.id_map: Dict[str, str] = {}

        # Create mapping directory if it doesn't exist
        self.mapping_dir.mkdir(exist_ok=True)

        # Load existing mapping if available
        self.load_mapping()

    def add_mapping(self, source_id: str, dest_id: str) -> None:
        """Add a new ID mapping."""
        self.id_map[str(source_id)] = str(dest_id)
        self.save_mapping()

    def get_dest_id(self, source_id: str) -> Optional[str]:
        """Get destination ID for a given source ID."""
        return self.id_map.get(str(source_id))

    def get_source_id(self, dest_id: str) -> Optional[str]:
        """Get source ID for a given destination ID."""
        for src, dst in self.id_map.items():
            if dst == str(dest_id):
                return src
        return None

    def load_mapping(self) -> None:
        """Load mapping from file if it exists."""
        try:
            if self.mapping_file.exists():
                with open(self.mapping_file, "r") as f:
                    self.id_map = json.load(f)
        except Exception as e:
            print(f"Error loading mapping file: {e}")
            self.id_map = {}

    def save_mapping(self) -> None:
        """Save current mapping to file."""
        try:
            with open(self.mapping_file, "w") as f:
                json.dump(self.id_map, f, indent=2)
        except Exception as e:
            print(f"Error saving mapping file: {e}")

    def clear_mapping(self) -> None:
        """Clear all mappings."""
        self.id_map = {}
        self.save_mapping()

    def get_all_mappings(self) -> Dict[str, str]:
        """Get all mappings."""
        return self.id_map.copy()


# Global instance for ID mapping
user_id_mapper = IDMapper("users")
dealer_id_mapper = IDMapper("dealers")
customer_id_mapper = IDMapper("customers")
