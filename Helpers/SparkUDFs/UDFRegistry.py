"""
UDF Registry System for Centralized UDF Management

This module provides a centralized registry for managing all PySpark UDFs used in the
Connected Driving Pipeline. It enables:
- Automatic UDF discovery and registration
- Metadata tracking (description, input/output types, categories)
- Easy UDF retrieval by name or category
- Documentation generation

Author: PySpark Migration Team
Created: 2026-01-17
"""

from typing import Dict, List, Optional, Callable, Any
from pyspark.sql.functions import udf
from dataclasses import dataclass, field
from enum import Enum


class UDFCategory(Enum):
    """Categories for organizing UDFs"""
    GEOSPATIAL = "geospatial"
    CONVERSION = "conversion"
    TEMPORAL = "temporal"
    ATTACK = "attack"
    UTILITY = "utility"


@dataclass
class UDFMetadata:
    """Metadata for a registered UDF"""
    name: str
    udf_func: Callable
    description: str
    category: UDFCategory
    input_types: List[str] = field(default_factory=list)
    output_type: str = ""
    example: str = ""
    version: str = "1.0.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for serialization"""
        return {
            'name': self.name,
            'description': self.description,
            'category': self.category.value,
            'input_types': self.input_types,
            'output_type': self.output_type,
            'example': self.example,
            'version': self.version
        }


class UDFRegistry:
    """
    Centralized registry for managing PySpark UDFs.

    This singleton class provides a central location for:
    - Registering UDFs with metadata
    - Retrieving UDFs by name
    - Listing UDFs by category
    - Generating documentation

    Example:
        >>> registry = UDFRegistry.get_instance()
        >>> udf_func = registry.get('hex_to_decimal')
        >>> df = df.withColumn('id_decimal', udf_func(col('id_hex')))

        >>> # List all geospatial UDFs
        >>> geo_udfs = registry.list_by_category(UDFCategory.GEOSPATIAL)
    """

    _instance: Optional['UDFRegistry'] = None

    def __init__(self):
        """Initialize the UDF registry. Use get_instance() instead of direct instantiation."""
        if UDFRegistry._instance is not None:
            raise RuntimeError("UDFRegistry is a singleton. Use get_instance() instead.")
        self._udfs: Dict[str, UDFMetadata] = {}
        self._initialized = False

    @classmethod
    def get_instance(cls) -> 'UDFRegistry':
        """
        Get the singleton instance of the UDF registry.

        Returns:
            The singleton UDFRegistry instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """
        Reset the singleton instance (useful for testing).
        WARNING: Only use this in test environments.
        """
        cls._instance = None

    def register(self,
                 name: str,
                 udf_func: Callable,
                 description: str,
                 category: UDFCategory,
                 input_types: Optional[List[str]] = None,
                 output_type: str = "",
                 example: str = "",
                 version: str = "1.0.0") -> None:
        """
        Register a UDF with metadata.

        Args:
            name: Unique identifier for the UDF
            udf_func: The actual PySpark UDF function
            description: Human-readable description of what the UDF does
            category: Category for organizing UDFs
            input_types: List of input parameter types (e.g., ['string', 'int'])
            output_type: Return type of the UDF (e.g., 'double', 'struct')
            example: Usage example as a string
            version: Version string (default: "1.0.0")

        Raises:
            ValueError: If a UDF with the same name is already registered
        """
        if name in self._udfs:
            raise ValueError(f"UDF '{name}' is already registered")

        metadata = UDFMetadata(
            name=name,
            udf_func=udf_func,
            description=description,
            category=category,
            input_types=input_types or [],
            output_type=output_type,
            example=example,
            version=version
        )

        self._udfs[name] = metadata

    def get(self, name: str) -> Callable:
        """
        Retrieve a UDF by name.

        Args:
            name: Name of the UDF to retrieve

        Returns:
            The PySpark UDF function

        Raises:
            KeyError: If the UDF is not registered
        """
        if name not in self._udfs:
            raise KeyError(f"UDF '{name}' is not registered. Available UDFs: {self.list_all()}")
        return self._udfs[name].udf_func

    def get_metadata(self, name: str) -> UDFMetadata:
        """
        Retrieve metadata for a UDF.

        Args:
            name: Name of the UDF

        Returns:
            UDFMetadata object containing information about the UDF

        Raises:
            KeyError: If the UDF is not registered
        """
        if name not in self._udfs:
            raise KeyError(f"UDF '{name}' is not registered")
        return self._udfs[name]

    def list_all(self) -> List[str]:
        """
        List all registered UDF names.

        Returns:
            List of UDF names sorted alphabetically
        """
        return sorted(self._udfs.keys())

    def list_by_category(self, category: UDFCategory) -> List[str]:
        """
        List all UDFs in a specific category.

        Args:
            category: The UDFCategory to filter by

        Returns:
            List of UDF names in the specified category, sorted alphabetically
        """
        return sorted([
            name for name, metadata in self._udfs.items()
            if metadata.category == category
        ])

    def get_categories(self) -> List[UDFCategory]:
        """
        Get all categories that have registered UDFs.

        Returns:
            List of UDFCategory enums that have at least one registered UDF
        """
        categories = set(metadata.category for metadata in self._udfs.values())
        return sorted(categories, key=lambda c: c.value)

    def exists(self, name: str) -> bool:
        """
        Check if a UDF is registered.

        Args:
            name: Name of the UDF to check

        Returns:
            True if the UDF is registered, False otherwise
        """
        return name in self._udfs

    def count(self) -> int:
        """
        Get the total number of registered UDFs.

        Returns:
            Number of registered UDFs
        """
        return len(self._udfs)

    def generate_documentation(self) -> str:
        """
        Generate human-readable documentation for all registered UDFs.

        Returns:
            Formatted string containing documentation for all UDFs
        """
        doc_lines = ["# PySpark UDF Registry Documentation\n"]
        doc_lines.append(f"Total UDFs: {self.count()}\n")

        for category in self.get_categories():
            doc_lines.append(f"\n## {category.value.upper()}\n")
            udf_names = self.list_by_category(category)

            for name in udf_names:
                metadata = self._udfs[name]
                doc_lines.append(f"\n### {metadata.name} (v{metadata.version})")
                doc_lines.append(f"**Description:** {metadata.description}")

                if metadata.input_types:
                    doc_lines.append(f"**Input Types:** {', '.join(metadata.input_types)}")
                if metadata.output_type:
                    doc_lines.append(f"**Output Type:** {metadata.output_type}")
                if metadata.example:
                    doc_lines.append(f"**Example:**\n```python\n{metadata.example}\n```")
                doc_lines.append("")

        return "\n".join(doc_lines)

    def is_initialized(self) -> bool:
        """
        Check if the registry has been initialized with UDFs.

        Returns:
            True if UDFs have been registered, False otherwise
        """
        return self._initialized

    def mark_initialized(self):
        """Mark the registry as initialized (called after auto-registration)"""
        self._initialized = True


def get_registry() -> UDFRegistry:
    """
    Convenience function to get the singleton UDF registry instance.

    Returns:
        The singleton UDFRegistry instance
    """
    return UDFRegistry.get_instance()
