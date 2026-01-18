"""
Schema Evolution Utilities for PySpark DataFrames

This module provides comprehensive schema evolution capabilities to handle
schema changes over time, enabling backward and forward compatibility when
working with evolving data sources.

Key Features:
- Add missing columns with configurable default values
- Remove extra columns automatically
- Type casting and coercion between compatible types
- Multiple evolution modes (strict, lenient, additive)
- Schema versioning and migration tracking
- Backward compatibility validation

Author: PySpark Migration Team
Date: January 2026
"""

from typing import Optional, Dict, Any, List, Callable
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, DataType, StringType, IntegerType,
    LongType, DoubleType, FloatType, BooleanType, TimestampType,
    DateType, ByteType, ShortType
)
from pyspark.sql.functions import lit, col, when
import logging

logger = logging.getLogger(__name__)


class EvolutionMode(Enum):
    """
    Schema evolution modes defining how to handle schema differences.

    STRICT: Fail on any schema differences (missing, extra, type mismatches)
    LENIENT: Allow all differences, evolve schema automatically
    ADDITIVE: Allow new columns in source, but fail on missing required columns
    TYPE_SAFE: Allow column additions/removals but fail on type mismatches
    """
    STRICT = "strict"
    LENIENT = "lenient"
    ADDITIVE = "additive"
    TYPE_SAFE = "type_safe"


class SchemaEvolutionError(Exception):
    """Exception raised when schema evolution fails."""
    pass


class SchemaEvolver:
    """
    Handles schema evolution for PySpark DataFrames.

    This class provides methods to evolve DataFrame schemas to match expected
    schemas, handling missing columns, extra columns, and type conversions
    automatically based on configured evolution modes.

    Example:
        >>> evolver = SchemaEvolver(mode=EvolutionMode.LENIENT)
        >>> evolved_df = evolver.evolve(df, target_schema)
    """

    def __init__(
        self,
        mode: EvolutionMode = EvolutionMode.LENIENT,
        default_values: Optional[Dict[str, Any]] = None,
        type_casters: Optional[Dict[str, Callable]] = None
    ):
        """
        Initialize SchemaEvolver.

        Args:
            mode: Evolution mode (strict, lenient, additive, type_safe)
            default_values: Dict mapping column names to default values for missing columns
            type_casters: Dict mapping type pairs to custom casting functions
        """
        self.mode = mode
        self.default_values = default_values or {}
        self.type_casters = type_casters or {}

    def evolve(
        self,
        df: DataFrame,
        target_schema: StructType,
        raise_on_error: bool = True
    ) -> DataFrame:
        """
        Evolve a DataFrame's schema to match the target schema.

        This is the main method that orchestrates the evolution process:
        1. Detect schema differences
        2. Apply evolution strategy based on mode
        3. Add missing columns with defaults
        4. Remove extra columns if allowed
        5. Cast incompatible types if possible

        Args:
            df: Source DataFrame to evolve
            target_schema: Target schema to evolve toward
            raise_on_error: If True, raise exception on evolution failures

        Returns:
            DataFrame: Evolved DataFrame matching target schema

        Raises:
            SchemaEvolutionError: If evolution fails and raise_on_error is True

        Example:
            >>> from Schemas import get_bsm_raw_schema
            >>> target = get_bsm_raw_schema()
            >>> evolver = SchemaEvolver(EvolutionMode.LENIENT)
            >>> evolved_df = evolver.evolve(df, target)
        """
        try:
            logger.info(f"Evolving schema with mode={self.mode.value}")

            # Get schema differences
            from Helpers.SchemaValidator import SchemaValidator
            diff = SchemaValidator.get_schema_diff(df, target_schema)

            # Check mode compatibility
            if not self._validate_mode_compatibility(diff, raise_on_error):
                # Validation failed and raise_on_error=False
                logger.warning("Schema evolution validation failed, returning original DataFrame")
                return df

            # Apply evolution transformations
            evolved_df = df

            # Step 1: Add missing columns
            if diff['missing_columns']:
                evolved_df = self._add_missing_columns(
                    evolved_df, target_schema, diff['missing_columns']
                )

            # Step 2: Cast type mismatches
            if diff['type_mismatches']:
                evolved_df = self._cast_type_mismatches(
                    evolved_df, target_schema, diff['type_mismatches']
                )

            # Step 3: Remove extra columns (if allowed by mode)
            if diff['extra_columns']:
                evolved_df = self._remove_extra_columns(
                    evolved_df, diff['extra_columns']
                )

            # Step 4: Reorder columns to match target schema
            evolved_df = self._reorder_columns(evolved_df, target_schema)

            logger.info("Schema evolution completed successfully")
            return evolved_df

        except SchemaEvolutionError:
            # Re-raise SchemaEvolutionError as-is
            raise
        except Exception as e:
            if raise_on_error:
                raise SchemaEvolutionError(f"Schema evolution failed: {str(e)}") from e
            else:
                logger.error(f"Schema evolution failed: {str(e)}")
                return df

    def _validate_mode_compatibility(self, diff: Dict[str, Any], raise_on_error: bool) -> bool:
        """
        Validate that schema differences are compatible with evolution mode.

        Args:
            diff: Schema diff from SchemaValidator.get_schema_diff()
            raise_on_error: Whether to raise exception on incompatibility

        Returns:
            bool: True if compatible, False otherwise

        Raises:
            SchemaEvolutionError: If differences violate mode constraints
        """
        errors = []

        if self.mode == EvolutionMode.STRICT:
            # STRICT: No differences allowed
            if diff['missing_columns']:
                errors.append(f"STRICT mode: Missing columns not allowed: {diff['missing_columns']}")
            if diff['extra_columns']:
                errors.append(f"STRICT mode: Extra columns not allowed: {diff['extra_columns']}")
            if diff['type_mismatches']:
                errors.append(f"STRICT mode: Type mismatches not allowed: {list(diff['type_mismatches'].keys())}")

        elif self.mode == EvolutionMode.ADDITIVE:
            # ADDITIVE: Allow extra columns, but not missing columns
            if diff['missing_columns']:
                errors.append(f"ADDITIVE mode: Missing required columns: {diff['missing_columns']}")

        elif self.mode == EvolutionMode.TYPE_SAFE:
            # TYPE_SAFE: Allow column changes, but not type changes
            if diff['type_mismatches']:
                # Check if types are compatible (can be cast)
                incompatible = []
                for col_name, (expected_type, actual_type) in diff['type_mismatches'].items():
                    if not self._can_cast_types(actual_type, expected_type):
                        incompatible.append(f"{col_name}: {actual_type} -> {expected_type}")

                if incompatible:
                    errors.append(f"TYPE_SAFE mode: Incompatible type changes: {incompatible}")

        # LENIENT: Allow everything (no validation needed)

        if errors:
            if raise_on_error:
                raise SchemaEvolutionError("\n".join(errors))
            return False

        return True

    def _add_missing_columns(
        self,
        df: DataFrame,
        target_schema: StructType,
        missing_columns: List[str]
    ) -> DataFrame:
        """
        Add missing columns to DataFrame with default values.

        Args:
            df: Source DataFrame
            target_schema: Target schema containing column definitions
            missing_columns: List of column names to add

        Returns:
            DataFrame with missing columns added
        """
        result_df = df
        target_fields = {field.name: field for field in target_schema.fields}

        for col_name in missing_columns:
            field = target_fields[col_name]
            default_value = self._get_default_value(col_name, field.dataType)

            # Add column with default value
            result_df = result_df.withColumn(col_name, lit(default_value).cast(field.dataType))
            logger.info(f"Added missing column '{col_name}' with default value: {default_value}")

        return result_df

    def _get_default_value(self, col_name: str, data_type: DataType) -> Any:
        """
        Get default value for a column based on configuration or type.

        Args:
            col_name: Column name
            data_type: Column data type

        Returns:
            Default value for the column
        """
        # Check explicit default values first
        if col_name in self.default_values:
            return self.default_values[col_name]

        # Otherwise, use type-based defaults
        type_defaults = {
            StringType: None,
            IntegerType: 0,
            LongType: 0,
            DoubleType: 0.0,
            FloatType: 0.0,
            BooleanType: False,
            ByteType: 0,
            ShortType: 0,
            TimestampType: None,
            DateType: None
        }

        for type_class, default_val in type_defaults.items():
            if isinstance(data_type, type_class):
                return default_val

        # Default to None for unknown types
        return None

    def _cast_type_mismatches(
        self,
        df: DataFrame,
        target_schema: StructType,
        type_mismatches: Dict[str, tuple]
    ) -> DataFrame:
        """
        Cast columns to target types where possible.

        Args:
            df: Source DataFrame
            target_schema: Target schema
            type_mismatches: Dict of column names to (expected, actual) type strings

        Returns:
            DataFrame with type casts applied
        """
        result_df = df
        target_fields = {field.name: field for field in target_schema.fields}

        for col_name in type_mismatches.keys():
            target_field = target_fields[col_name]
            target_type = target_field.dataType

            # Apply type cast
            result_df = result_df.withColumn(
                col_name,
                col(col_name).cast(target_type)
            )
            logger.info(f"Cast column '{col_name}' to {target_type.simpleString()}")

        return result_df

    def _can_cast_types(self, from_type: str, to_type: str) -> bool:
        """
        Check if a type can be safely cast to another type.

        Args:
            from_type: Source type (simple string)
            to_type: Target type (simple string)

        Returns:
            bool: True if cast is possible
        """
        # Same type
        if from_type == to_type:
            return True

        # Numeric type conversions
        numeric_hierarchy = ['byte', 'short', 'int', 'long', 'float', 'double', 'decimal']

        try:
            from_idx = numeric_hierarchy.index(from_type)
            to_idx = numeric_hierarchy.index(to_type)
            # Allow widening conversions (smaller -> larger)
            return from_idx <= to_idx
        except ValueError:
            pass

        # String conversions (string can be cast to most things, with potential data loss)
        if from_type == 'string':
            return True

        # Allow int <-> long
        if {from_type, to_type} <= {'int', 'long'}:
            return True

        # Allow float <-> double
        if {from_type, to_type} <= {'float', 'double'}:
            return True

        return False

    def _remove_extra_columns(
        self,
        df: DataFrame,
        extra_columns: List[str]
    ) -> DataFrame:
        """
        Remove extra columns from DataFrame.

        Args:
            df: Source DataFrame
            extra_columns: List of column names to remove

        Returns:
            DataFrame with extra columns removed
        """
        if self.mode == EvolutionMode.ADDITIVE:
            # ADDITIVE mode keeps extra columns
            logger.info(f"ADDITIVE mode: Keeping extra columns {extra_columns}")
            return df

        result_df = df.drop(*extra_columns)
        logger.info(f"Removed extra columns: {extra_columns}")
        return result_df

    def _reorder_columns(
        self,
        df: DataFrame,
        target_schema: StructType
    ) -> DataFrame:
        """
        Reorder DataFrame columns to match target schema.

        Args:
            df: Source DataFrame
            target_schema: Target schema with desired column order

        Returns:
            DataFrame with columns reordered
        """
        # Get target column order (only include columns that exist in df)
        target_columns = [field.name for field in target_schema.fields]
        existing_columns = set(df.columns)

        # Select columns in target order (only those that exist)
        ordered_columns = [c for c in target_columns if c in existing_columns]

        # Add any extra columns at the end (for ADDITIVE mode)
        extra_columns = [c for c in df.columns if c not in ordered_columns]
        final_order = ordered_columns + extra_columns

        return df.select(*final_order)


class SchemaVersion:
    """
    Represents a versioned schema with migration capabilities.

    This class tracks schema versions and provides mechanisms for
    migrating data between versions.

    Example:
        >>> v1 = SchemaVersion(version=1, schema=schema_v1)
        >>> v2 = SchemaVersion(version=2, schema=schema_v2)
        >>> migrated_df = v2.migrate_from(df, source_version=v1)
    """

    def __init__(
        self,
        version: int,
        schema: StructType,
        description: Optional[str] = None,
        migration_rules: Optional[Dict[int, Callable]] = None
    ):
        """
        Initialize a SchemaVersion.

        Args:
            version: Schema version number
            schema: PySpark StructType schema
            description: Human-readable description of this version
            migration_rules: Dict mapping source version -> migration function
        """
        self.version = version
        self.schema = schema
        self.description = description
        self.migration_rules = migration_rules or {}

    def migrate_from(
        self,
        df: DataFrame,
        source_version: int,
        evolver: Optional[SchemaEvolver] = None
    ) -> DataFrame:
        """
        Migrate a DataFrame from a source version to this version.

        Args:
            df: Source DataFrame
            source_version: Version number of source DataFrame
            evolver: SchemaEvolver to use (default: LENIENT mode)

        Returns:
            DataFrame migrated to this version's schema

        Example:
            >>> v2 = SchemaVersion(version=2, schema=schema_v2)
            >>> migrated_df = v2.migrate_from(old_df, source_version=1)
        """
        if source_version == self.version:
            logger.info(f"No migration needed (already version {self.version})")
            return df

        # Check for custom migration rule
        if source_version in self.migration_rules:
            logger.info(f"Applying custom migration: v{source_version} -> v{self.version}")
            return self.migration_rules[source_version](df)

        # Default: Use SchemaEvolver
        if evolver is None:
            evolver = SchemaEvolver(mode=EvolutionMode.LENIENT)

        logger.info(f"Migrating schema: v{source_version} -> v{self.version}")
        return evolver.evolve(df, self.schema)

    def add_migration_rule(
        self,
        source_version: int,
        migration_func: Callable[[DataFrame], DataFrame]
    ):
        """
        Add a custom migration rule from a specific source version.

        Args:
            source_version: Source version number
            migration_func: Function that takes DataFrame and returns migrated DataFrame

        Example:
            >>> def migrate_v1_to_v2(df):
            ...     return df.withColumn('new_col', lit('default'))
            >>> v2.add_migration_rule(1, migrate_v1_to_v2)
        """
        self.migration_rules[source_version] = migration_func
        logger.info(f"Added migration rule: v{source_version} -> v{self.version}")


def evolve_to_schema(
    df: DataFrame,
    target_schema: StructType,
    mode: EvolutionMode = EvolutionMode.LENIENT,
    default_values: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Convenience function to evolve a DataFrame to a target schema.

    Args:
        df: Source DataFrame
        target_schema: Target schema
        mode: Evolution mode
        default_values: Default values for missing columns

    Returns:
        Evolved DataFrame

    Example:
        >>> from Helpers.SchemaEvolution import evolve_to_schema, EvolutionMode
        >>> evolved = evolve_to_schema(df, target_schema, EvolutionMode.LENIENT)
    """
    evolver = SchemaEvolver(mode=mode, default_values=default_values)
    return evolver.evolve(df, target_schema)


def validate_backward_compatibility(
    old_schema: StructType,
    new_schema: StructType
) -> tuple[bool, List[str]]:
    """
    Validate that a new schema is backward compatible with an old schema.

    Backward compatibility means:
    - All columns in old schema exist in new schema (or can be added with defaults)
    - Column types are the same or can be safely widened
    - No breaking changes that would prevent reading old data

    Args:
        old_schema: Old/previous schema
        new_schema: New/updated schema

    Returns:
        Tuple of (is_compatible, list_of_breaking_changes)

    Example:
        >>> compatible, issues = validate_backward_compatibility(schema_v1, schema_v2)
        >>> if not compatible:
        ...     print("Breaking changes:", issues)
    """
    breaking_changes = []

    old_fields = {field.name: field for field in old_schema.fields}
    new_fields = {field.name: field for field in new_schema.fields}

    # Check for removed columns (breaking change)
    removed_columns = set(old_fields.keys()) - set(new_fields.keys())
    if removed_columns:
        breaking_changes.append(f"Removed columns: {sorted(removed_columns)}")

    # Check for type changes in existing columns
    for col_name in set(old_fields.keys()) & set(new_fields.keys()):
        old_field = old_fields[col_name]
        new_field = new_fields[col_name]

        # Type must be the same or safely widened
        if old_field.dataType != new_field.dataType:
            # Check if it's a safe widening conversion
            old_type = old_field.dataType.typeName()
            new_type = new_field.dataType.typeName()

            # Check compatibility using type hierarchy
            compatible = _is_type_widening_safe(old_type, new_type)

            if not compatible:
                breaking_changes.append(
                    f"Column '{col_name}': Incompatible type change "
                    f"{old_type} -> {new_type}"
                )

        # Changing nullable=True to nullable=False is breaking
        if old_field.nullable and not new_field.nullable:
            breaking_changes.append(
                f"Column '{col_name}': Changed from nullable to non-nullable (breaking)"
            )

    is_compatible = len(breaking_changes) == 0
    return is_compatible, breaking_changes


def _is_type_widening_safe(old_type: str, new_type: str) -> bool:
    """
    Check if type change is a safe widening conversion.

    Args:
        old_type: Old type name (e.g., 'integer', 'float')
        new_type: New type name (e.g., 'long', 'double')

    Returns:
        bool: True if widening is safe
    """
    # Same type is always compatible
    if old_type == new_type:
        return True

    # Numeric type hierarchy (smaller to larger is safe)
    numeric_hierarchy = ['byte', 'short', 'integer', 'long', 'float', 'double', 'decimal']

    try:
        old_idx = numeric_hierarchy.index(old_type)
        new_idx = numeric_hierarchy.index(new_type)
        # Allow widening (smaller -> larger)
        return old_idx <= new_idx
    except ValueError:
        pass

    # String type variations
    string_types = {'string', 'varchar', 'char'}
    if old_type in string_types and new_type in string_types:
        return True

    # Allow integer <-> long specifically
    if {old_type, new_type} <= {'integer', 'long'}:
        return True

    # Allow float <-> double specifically
    if {old_type, new_type} <= {'float', 'double'}:
        return True

    return False
