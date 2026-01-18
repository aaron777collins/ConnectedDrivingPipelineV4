"""
Schema Validation Utilities for PySpark DataFrames

This module provides utilities to validate PySpark DataFrames against expected schemas,
ensuring data quality and compatibility throughout the pipeline.
"""

from typing import List, Dict, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType


class SchemaValidationError(Exception):
    """Custom exception for schema validation failures"""
    pass


class SchemaValidator:
    """
    Validates PySpark DataFrames against expected schemas.

    Provides comprehensive validation including:
    - Column name matching
    - Column type matching
    - Column order validation
    - Missing/extra column detection
    - Nullable constraint checking
    """

    @staticmethod
    def validate_schema(df: DataFrame,
                       expected_schema: StructType,
                       strict_order: bool = False,
                       strict_nullability: bool = False,
                       raise_on_error: bool = True) -> Tuple[bool, List[str]]:
        """
        Validate a DataFrame's schema against an expected schema.

        Args:
            df: The DataFrame to validate
            expected_schema: The expected StructType schema
            strict_order: If True, columns must be in the same order
            strict_nullability: If True, nullable constraints must match exactly
            raise_on_error: If True, raise SchemaValidationError on validation failure

        Returns:
            Tuple of (is_valid, list_of_errors)

        Raises:
            SchemaValidationError: If validation fails and raise_on_error is True
        """
        errors = []

        actual_schema = df.schema

        # Check column count
        if len(actual_schema.fields) != len(expected_schema.fields):
            errors.append(
                f"Column count mismatch: expected {len(expected_schema.fields)}, "
                f"got {len(actual_schema.fields)}"
            )

        # Build maps for comparison
        expected_fields = {field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}

        # Check for missing columns
        missing_cols = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing_cols:
            errors.append(f"Missing columns: {sorted(missing_cols)}")

        # Check for extra columns
        extra_cols = set(actual_fields.keys()) - set(expected_fields.keys())
        if extra_cols:
            errors.append(f"Extra columns: {sorted(extra_cols)}")

        # Check column order if strict
        if strict_order:
            expected_names = [f.name for f in expected_schema.fields]
            actual_names = [f.name for f in actual_schema.fields]
            if expected_names != actual_names:
                errors.append(
                    f"Column order mismatch:\n"
                    f"  Expected: {expected_names}\n"
                    f"  Actual:   {actual_names}"
                )

        # Check column types and nullability for matching columns
        for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_field = expected_fields[col_name]
            actual_field = actual_fields[col_name]

            # Check data type
            if not SchemaValidator._types_match(expected_field.dataType, actual_field.dataType):
                errors.append(
                    f"Column '{col_name}' type mismatch: "
                    f"expected {expected_field.dataType.simpleString()}, "
                    f"got {actual_field.dataType.simpleString()}"
                )

            # Check nullability if strict
            if strict_nullability and expected_field.nullable != actual_field.nullable:
                errors.append(
                    f"Column '{col_name}' nullability mismatch: "
                    f"expected nullable={expected_field.nullable}, "
                    f"got nullable={actual_field.nullable}"
                )

        is_valid = len(errors) == 0

        if not is_valid and raise_on_error:
            error_msg = "Schema validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise SchemaValidationError(error_msg)

        return is_valid, errors

    @staticmethod
    def _types_match(expected: DataType, actual: DataType) -> bool:
        """
        Check if two DataTypes match.

        Handles complex types (StructType, ArrayType, MapType) recursively.
        """
        # Direct type comparison
        if type(expected) != type(actual):
            return False

        # For simple types, the type check above is sufficient
        from pyspark.sql.types import StructType, ArrayType, MapType

        if isinstance(expected, StructType):
            # For structs, recursively check all fields
            if len(expected.fields) != len(actual.fields):
                return False
            for exp_field, act_field in zip(expected.fields, actual.fields):
                if exp_field.name != act_field.name:
                    return False
                if not SchemaValidator._types_match(exp_field.dataType, act_field.dataType):
                    return False
            return True

        elif isinstance(expected, ArrayType):
            return SchemaValidator._types_match(expected.elementType, actual.elementType)

        elif isinstance(expected, MapType):
            return (SchemaValidator._types_match(expected.keyType, actual.keyType) and
                   SchemaValidator._types_match(expected.valueType, actual.valueType))

        # For all other types, type equality check is sufficient
        return True

    @staticmethod
    def get_schema_diff(df: DataFrame, expected_schema: StructType) -> Dict[str, any]:
        """
        Get a detailed diff between DataFrame schema and expected schema.

        Args:
            df: The DataFrame to compare
            expected_schema: The expected schema

        Returns:
            Dictionary containing:
                - 'missing_columns': List of column names missing from DataFrame
                - 'extra_columns': List of extra column names in DataFrame
                - 'type_mismatches': Dict mapping column names to (expected, actual) types
                - 'nullability_mismatches': Dict mapping column names to (expected, actual) nullable values
        """
        actual_schema = df.schema

        expected_fields = {field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}

        diff = {
            'missing_columns': sorted(set(expected_fields.keys()) - set(actual_fields.keys())),
            'extra_columns': sorted(set(actual_fields.keys()) - set(expected_fields.keys())),
            'type_mismatches': {},
            'nullability_mismatches': {}
        }

        # Check for type and nullability mismatches in common columns
        for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_field = expected_fields[col_name]
            actual_field = actual_fields[col_name]

            if not SchemaValidator._types_match(expected_field.dataType, actual_field.dataType):
                diff['type_mismatches'][col_name] = (
                    expected_field.dataType.simpleString(),
                    actual_field.dataType.simpleString()
                )

            if expected_field.nullable != actual_field.nullable:
                diff['nullability_mismatches'][col_name] = (
                    expected_field.nullable,
                    actual_field.nullable
                )

        return diff

    @staticmethod
    def validate_columns_exist(df: DataFrame, required_columns: List[str],
                               raise_on_error: bool = True) -> Tuple[bool, List[str]]:
        """
        Validate that specific columns exist in a DataFrame.

        Args:
            df: The DataFrame to check
            required_columns: List of column names that must exist
            raise_on_error: If True, raise SchemaValidationError if columns are missing

        Returns:
            Tuple of (all_exist, list_of_missing_columns)

        Raises:
            SchemaValidationError: If columns are missing and raise_on_error is True
        """
        actual_columns = set(df.columns)
        required_set = set(required_columns)

        missing = sorted(required_set - actual_columns)
        all_exist = len(missing) == 0

        if not all_exist and raise_on_error:
            raise SchemaValidationError(
                f"Required columns missing from DataFrame: {missing}"
            )

        return all_exist, missing

    @staticmethod
    def validate_column_types(df: DataFrame,
                             column_types: Dict[str, DataType],
                             raise_on_error: bool = True) -> Tuple[bool, Dict[str, Tuple[str, str]]]:
        """
        Validate that specific columns have expected data types.

        Args:
            df: The DataFrame to check
            column_types: Dict mapping column names to expected DataTypes
            raise_on_error: If True, raise SchemaValidationError on type mismatches

        Returns:
            Tuple of (all_match, dict_of_mismatches)
            where dict_of_mismatches maps column names to (expected, actual) type strings

        Raises:
            SchemaValidationError: If type mismatches exist and raise_on_error is True
        """
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
        mismatches = {}

        for col_name, expected_type in column_types.items():
            if col_name not in schema_dict:
                mismatches[col_name] = (expected_type.simpleString(), "MISSING")
            elif not SchemaValidator._types_match(expected_type, schema_dict[col_name]):
                mismatches[col_name] = (
                    expected_type.simpleString(),
                    schema_dict[col_name].simpleString()
                )

        all_match = len(mismatches) == 0

        if not all_match and raise_on_error:
            error_lines = [f"Column type mismatches:"]
            for col, (exp, act) in mismatches.items():
                error_lines.append(f"  {col}: expected {exp}, got {act}")
            raise SchemaValidationError("\n".join(error_lines))

        return all_match, mismatches

    @staticmethod
    def print_schema_comparison(df: DataFrame, expected_schema: StructType):
        """
        Print a human-readable comparison of DataFrame schema vs expected schema.

        Args:
            df: The DataFrame to compare
            expected_schema: The expected schema
        """
        print("=" * 80)
        print("SCHEMA COMPARISON")
        print("=" * 80)

        diff = SchemaValidator.get_schema_diff(df, expected_schema)

        if not any([diff['missing_columns'], diff['extra_columns'],
                   diff['type_mismatches'], diff['nullability_mismatches']]):
            print("✓ Schemas match perfectly!")
            return

        if diff['missing_columns']:
            print("\nMISSING COLUMNS:")
            for col in diff['missing_columns']:
                print(f"  ✗ {col}")

        if diff['extra_columns']:
            print("\nEXTRA COLUMNS:")
            for col in diff['extra_columns']:
                print(f"  + {col}")

        if diff['type_mismatches']:
            print("\nTYPE MISMATCHES:")
            for col, (expected, actual) in diff['type_mismatches'].items():
                print(f"  ! {col}:")
                print(f"      Expected: {expected}")
                print(f"      Actual:   {actual}")

        if diff['nullability_mismatches']:
            print("\nNULLABILITY MISMATCHES:")
            for col, (expected, actual) in diff['nullability_mismatches'].items():
                print(f"  ! {col}: expected nullable={expected}, got nullable={actual}")

        print("=" * 80)


def validate_bsm_raw_schema(df: DataFrame, raise_on_error: bool = True) -> Tuple[bool, List[str]]:
    """
    Convenience function to validate a DataFrame against the BSM raw schema.

    Args:
        df: The DataFrame to validate
        raise_on_error: If True, raise SchemaValidationError on validation failure

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    from Schemas import get_bsm_raw_schema

    expected_schema = get_bsm_raw_schema()
    return SchemaValidator.validate_schema(df, expected_schema, raise_on_error=raise_on_error)


def validate_bsm_processed_schema(df: DataFrame, raise_on_error: bool = True) -> Tuple[bool, List[str]]:
    """
    Convenience function to validate a DataFrame against the BSM processed schema.

    Args:
        df: The DataFrame to validate
        raise_on_error: If True, raise SchemaValidationError on validation failure

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    from Schemas import get_bsm_processed_schema

    expected_schema = get_bsm_processed_schema()
    return SchemaValidator.validate_schema(df, expected_schema, raise_on_error=raise_on_error)
