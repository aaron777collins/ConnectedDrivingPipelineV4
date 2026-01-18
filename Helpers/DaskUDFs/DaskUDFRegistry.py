"""
Dask Function Registry System for Centralized Function Management

This module provides a centralized registry for managing all Dask functions used in the
Connected Driving Pipeline. It enables:
- Automatic function discovery and registration
- Metadata tracking (description, input/output types, categories)
- Easy function retrieval by name or category
- Documentation generation

Unlike PySpark's UDFRegistry which stores @udf-wrapped functions, DaskUDFRegistry
stores plain Python functions that work with pandas Series within Dask partitions.

Author: Dask Migration Team
Created: 2026-01-17
"""

from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum


class FunctionCategory(Enum):
    """Categories for organizing Dask functions"""
    GEOSPATIAL = "geospatial"
    CONVERSION = "conversion"
    TEMPORAL = "temporal"
    ATTACK = "attack"
    UTILITY = "utility"


@dataclass
class FunctionMetadata:
    """Metadata for a registered Dask function"""
    name: str
    func: Callable
    description: str
    category: FunctionCategory
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


class DaskUDFRegistry:
    """
    Centralized registry for managing Dask functions.

    This singleton class provides a central location for:
    - Registering functions with metadata
    - Retrieving functions by name
    - Listing functions by category
    - Generating documentation

    Unlike PySpark's UDFRegistry which stores @udf-decorated functions,
    DaskUDFRegistry stores plain Python functions designed to work with
    pandas Series within Dask partitions using map_partitions() or apply().

    Example:
        >>> registry = DaskUDFRegistry.get_instance()
        >>> func = registry.get('hex_to_decimal')
        >>> df = df.assign(id_decimal=df['id_hex'].apply(func, meta=('id_decimal', 'i8')))

        >>> # List all geospatial functions
        >>> geo_funcs = registry.list_by_category(FunctionCategory.GEOSPATIAL)
    """

    _instance: Optional['DaskUDFRegistry'] = None

    def __init__(self):
        """Initialize the function registry. Use get_instance() instead of direct instantiation."""
        if DaskUDFRegistry._instance is not None:
            raise RuntimeError("DaskUDFRegistry is a singleton. Use get_instance() instead.")
        self._functions: Dict[str, FunctionMetadata] = {}
        self._initialized = False

    @classmethod
    def get_instance(cls) -> 'DaskUDFRegistry':
        """
        Get the singleton instance of the function registry.

        Returns:
            The singleton DaskUDFRegistry instance
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
                 func: Callable,
                 description: str,
                 category: FunctionCategory,
                 input_types: Optional[List[str]] = None,
                 output_type: str = "",
                 example: str = "",
                 version: str = "1.0.0") -> None:
        """
        Register a Dask function with metadata.

        Args:
            name: Unique identifier for the function
            func: The actual Python function (not @udf-wrapped)
            description: Human-readable description of what the function does
            category: Category for organizing functions
            input_types: List of input parameter types (e.g., ['str', 'int'])
            output_type: Return type of the function (e.g., 'float', 'tuple')
            example: Usage example as a string
            version: Version string (default: "1.0.0")

        Raises:
            ValueError: If a function with the same name is already registered
        """
        if name in self._functions:
            raise ValueError(f"Function '{name}' is already registered")

        metadata = FunctionMetadata(
            name=name,
            func=func,
            description=description,
            category=category,
            input_types=input_types or [],
            output_type=output_type,
            example=example,
            version=version
        )

        self._functions[name] = metadata

    def get(self, name: str) -> Callable:
        """
        Retrieve a function by name.

        Args:
            name: Name of the function to retrieve

        Returns:
            The Python function

        Raises:
            KeyError: If the function is not registered
        """
        if name not in self._functions:
            raise KeyError(f"Function '{name}' is not registered. Available functions: {self.list_all()}")
        return self._functions[name].func

    def get_metadata(self, name: str) -> FunctionMetadata:
        """
        Retrieve metadata for a function.

        Args:
            name: Name of the function

        Returns:
            FunctionMetadata object containing information about the function

        Raises:
            KeyError: If the function is not registered
        """
        if name not in self._functions:
            raise KeyError(f"Function '{name}' is not registered")
        return self._functions[name]

    def list_all(self) -> List[str]:
        """
        List all registered function names.

        Returns:
            List of function names sorted alphabetically
        """
        return sorted(self._functions.keys())

    def list_by_category(self, category: FunctionCategory) -> List[str]:
        """
        List all functions in a specific category.

        Args:
            category: The FunctionCategory to filter by

        Returns:
            List of function names in the specified category, sorted alphabetically
        """
        return sorted([
            name for name, metadata in self._functions.items()
            if metadata.category == category
        ])

    def get_categories(self) -> List[FunctionCategory]:
        """
        Get all categories that have registered functions.

        Returns:
            List of FunctionCategory enums that have at least one registered function
        """
        categories = set(metadata.category for metadata in self._functions.values())
        return sorted(categories, key=lambda c: c.value)

    def exists(self, name: str) -> bool:
        """
        Check if a function is registered.

        Args:
            name: Name of the function to check

        Returns:
            True if the function is registered, False otherwise
        """
        return name in self._functions

    def count(self) -> int:
        """
        Get the total number of registered functions.

        Returns:
            Number of registered functions
        """
        return len(self._functions)

    def generate_documentation(self) -> str:
        """
        Generate human-readable documentation for all registered functions.

        Returns:
            Formatted string containing documentation for all functions
        """
        doc_lines = ["# Dask Function Registry Documentation\n"]
        doc_lines.append(f"Total Functions: {self.count()}\n")

        for category in self.get_categories():
            doc_lines.append(f"\n## {category.value.upper()}\n")
            func_names = self.list_by_category(category)

            for name in func_names:
                metadata = self._functions[name]
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
        Check if the registry has been initialized with functions.

        Returns:
            True if functions have been registered, False otherwise
        """
        return self._initialized

    def mark_initialized(self):
        """Mark the registry as initialized (called after auto-registration)"""
        self._initialized = True


def get_registry() -> DaskUDFRegistry:
    """
    Convenience function to get the singleton function registry instance.

    Returns:
        The singleton DaskUDFRegistry instance
    """
    return DaskUDFRegistry.get_instance()
