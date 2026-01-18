"""
Schema Inference Utilities for PySpark DataFrames

This module provides automatic schema inference capabilities for CSV and Parquet files,
enabling flexible data loading when explicit schemas are not available or when working
with unknown data sources.

Key Features:
- Automatic type detection from CSV/Parquet files
- Smart sampling for large files (infers from first N rows)
- Schema comparison and validation against expected schemas
- Fallback mechanisms for graceful handling of schema mismatches
- Support for custom type mappings and inference strategies

Author: PySpark Migration Team
Date: January 2026
"""

from typing import Optional, Dict, Any, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DataType
import logging

logger = logging.getLogger(__name__)


class SchemaInferenceError(Exception):
    """Exception raised when schema inference fails."""
    pass


class SchemaInferencer:
    """
    Utility class for automatic schema inference from CSV and Parquet files.

    This class provides methods to automatically detect data types and column
    structures from files, enabling flexible data loading without requiring
    explicit schema definitions.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> inferencer = SchemaInferencer(spark)
        >>> schema = inferencer.infer_from_csv("data.csv", sample_size=1000)
        >>> df = spark.read.schema(schema).csv("data.csv")
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the SchemaInferencer.

        Args:
            spark: Active SparkSession instance
        """
        self.spark = spark

    def infer_from_csv(
        self,
        filepath: str,
        sample_size: Optional[int] = 10000,
        header: bool = True,
        delimiter: str = ",",
        null_value: Optional[str] = None
    ) -> StructType:
        """
        Infer schema from a CSV file by sampling rows.

        This method reads a sample of the CSV file and uses PySpark's built-in
        schema inference to detect column types. For large files, sampling
        provides a performance benefit while maintaining accuracy.

        Args:
            filepath: Path to the CSV file
            sample_size: Number of rows to sample for inference (None = all rows)
            header: Whether the CSV has a header row
            delimiter: CSV field delimiter
            null_value: String representation of null values

        Returns:
            StructType: Inferred schema

        Raises:
            SchemaInferenceError: If inference fails or file cannot be read

        Example:
            >>> inferencer = SchemaInferencer(spark)
            >>> schema = inferencer.infer_from_csv("bsm_data.csv", sample_size=5000)
            >>> print(schema)
        """
        try:
            logger.info(f"Inferring schema from CSV: {filepath}")

            # Build CSV reader with inference enabled
            reader = self.spark.read \
                .option("header", str(header).lower()) \
                .option("inferSchema", "true") \
                .option("delimiter", delimiter)

            if null_value:
                reader = reader.option("nullValue", null_value)

            # Read CSV (with optional sampling)
            df = reader.csv(filepath)

            # Apply sampling if requested
            if sample_size is not None:
                df = df.limit(sample_size)

            # Trigger schema inference by accessing the schema
            schema = df.schema

            logger.info(f"Inferred schema with {len(schema.fields)} columns")
            logger.debug(f"Schema: {schema}")

            return schema

        except Exception as e:
            raise SchemaInferenceError(
                f"Failed to infer schema from CSV '{filepath}': {str(e)}"
            ) from e

    def infer_from_parquet(self, filepath: str) -> StructType:
        """
        Infer schema from a Parquet file.

        Parquet files have embedded schemas, so this method simply reads
        the schema metadata without loading the data.

        Args:
            filepath: Path to the Parquet file or directory

        Returns:
            StructType: Schema from Parquet metadata

        Raises:
            SchemaInferenceError: If schema cannot be read

        Example:
            >>> inferencer = SchemaInferencer(spark)
            >>> schema = inferencer.infer_from_parquet("cache/data.parquet")
        """
        try:
            logger.info(f"Reading schema from Parquet: {filepath}")

            # Parquet files have embedded schemas - no inference needed
            df = self.spark.read.parquet(filepath)
            schema = df.schema

            logger.info(f"Read schema with {len(schema.fields)} columns")
            return schema

        except Exception as e:
            raise SchemaInferenceError(
                f"Failed to read schema from Parquet '{filepath}': {str(e)}"
            ) from e

    def infer_with_fallback(
        self,
        filepath: str,
        expected_schema: Optional[StructType] = None,
        sample_size: Optional[int] = 10000,
        header: bool = True
    ) -> StructType:
        """
        Infer schema with fallback to expected schema if inference fails.

        This method attempts to infer the schema automatically, but falls back
        to a provided expected schema if inference fails. Useful for robust
        data loading pipelines.

        Args:
            filepath: Path to the file (CSV or Parquet)
            expected_schema: Fallback schema to use if inference fails
            sample_size: Number of rows to sample for CSV inference
            header: Whether CSV has header row

        Returns:
            StructType: Inferred schema or fallback schema

        Example:
            >>> from Schemas.BSMRawSchema import get_bsm_raw_schema
            >>> schema = inferencer.infer_with_fallback(
            ...     "data.csv",
            ...     expected_schema=get_bsm_raw_schema()
            ... )
        """
        try:
            # Try to infer based on file extension
            if filepath.endswith('.parquet'):
                return self.infer_from_parquet(filepath)
            else:
                return self.infer_from_csv(
                    filepath,
                    sample_size=sample_size,
                    header=header
                )
        except SchemaInferenceError as e:
            if expected_schema is not None:
                logger.warning(
                    f"Schema inference failed, using fallback schema: {str(e)}"
                )
                return expected_schema
            else:
                raise

    def compare_schemas(
        self,
        inferred_schema: StructType,
        expected_schema: StructType,
        strict_types: bool = False
    ) -> Tuple[bool, List[str]]:
        """
        Compare an inferred schema with an expected schema.

        This method validates that an inferred schema is compatible with
        an expected schema, checking column names and optionally types.

        Args:
            inferred_schema: The automatically inferred schema
            expected_schema: The expected/reference schema
            strict_types: If True, require exact type matches; if False,
                         allow compatible types (e.g., int vs long)

        Returns:
            Tuple[bool, List[str]]: (is_compatible, list_of_differences)

        Example:
            >>> inferred = inferencer.infer_from_csv("data.csv")
            >>> expected = get_bsm_raw_schema()
            >>> compatible, diffs = inferencer.compare_schemas(inferred, expected)
            >>> if not compatible:
            ...     print("Schema differences:", diffs)
        """
        differences = []

        # Check column count
        if len(inferred_schema.fields) != len(expected_schema.fields):
            differences.append(
                f"Column count mismatch: inferred={len(inferred_schema.fields)}, "
                f"expected={len(expected_schema.fields)}"
            )

        # Build field maps for comparison
        inferred_map = {field.name: field for field in inferred_schema.fields}
        expected_map = {field.name: field for field in expected_schema.fields}

        # Check for missing columns
        missing_cols = set(expected_map.keys()) - set(inferred_map.keys())
        if missing_cols:
            differences.append(f"Missing columns: {sorted(missing_cols)}")

        # Check for extra columns
        extra_cols = set(inferred_map.keys()) - set(expected_map.keys())
        if extra_cols:
            differences.append(f"Extra columns: {sorted(extra_cols)}")

        # Check column types for matching columns
        common_cols = set(inferred_map.keys()) & set(expected_map.keys())
        for col_name in sorted(common_cols):
            inferred_field = inferred_map[col_name]
            expected_field = expected_map[col_name]

            if strict_types:
                # Exact type match required
                if inferred_field.dataType != expected_field.dataType:
                    differences.append(
                        f"Column '{col_name}' type mismatch: "
                        f"inferred={inferred_field.dataType}, "
                        f"expected={expected_field.dataType}"
                    )
            else:
                # Allow compatible types (e.g., int/long, float/double)
                if not self._types_compatible(
                    inferred_field.dataType,
                    expected_field.dataType
                ):
                    differences.append(
                        f"Column '{col_name}' incompatible types: "
                        f"inferred={inferred_field.dataType}, "
                        f"expected={expected_field.dataType}"
                    )

        is_compatible = len(differences) == 0
        return is_compatible, differences

    def _types_compatible(self, type1: DataType, type2: DataType) -> bool:
        """
        Check if two data types are compatible (not necessarily identical).

        Args:
            type1: First data type
            type2: Second data type

        Returns:
            bool: True if types are compatible
        """
        # Exact match
        if type1 == type2:
            return True

        # Get type names for comparison
        type1_name = type1.typeName()
        type2_name = type2.typeName()

        # Integer types are compatible
        int_types = {'byte', 'short', 'integer', 'long'}
        if type1_name in int_types and type2_name in int_types:
            return True

        # Floating point types are compatible
        float_types = {'float', 'double', 'decimal'}
        if type1_name in float_types and type2_name in float_types:
            return True

        # String types
        string_types = {'string', 'varchar', 'char'}
        if type1_name in string_types and type2_name in string_types:
            return True

        return False

    def get_schema_summary(self, schema: StructType) -> Dict[str, Any]:
        """
        Get a human-readable summary of a schema.

        Args:
            schema: Schema to summarize

        Returns:
            Dict with schema statistics and field information

        Example:
            >>> summary = inferencer.get_schema_summary(schema)
            >>> print(f"Columns: {summary['column_count']}")
            >>> print(f"Nullable columns: {summary['nullable_count']}")
        """
        fields = schema.fields

        type_counts = {}
        for field in fields:
            type_name = field.dataType.typeName()
            type_counts[type_name] = type_counts.get(type_name, 0) + 1

        nullable_count = sum(1 for f in fields if f.nullable)

        return {
            'column_count': len(fields),
            'column_names': [f.name for f in fields],
            'type_counts': type_counts,
            'nullable_count': nullable_count,
            'non_nullable_count': len(fields) - nullable_count,
            'fields': [
                {
                    'name': f.name,
                    'type': f.dataType.typeName(),
                    'nullable': f.nullable
                }
                for f in fields
            ]
        }


def infer_csv_schema(
    spark: SparkSession,
    filepath: str,
    sample_size: Optional[int] = 10000,
    header: bool = True
) -> StructType:
    """
    Convenience function to infer schema from CSV file.

    Args:
        spark: Active SparkSession
        filepath: Path to CSV file
        sample_size: Number of rows to sample
        header: Whether CSV has header row

    Returns:
        StructType: Inferred schema

    Example:
        >>> from Helpers.SchemaInferencer import infer_csv_schema
        >>> schema = infer_csv_schema(spark, "data.csv", sample_size=5000)
    """
    inferencer = SchemaInferencer(spark)
    return inferencer.infer_from_csv(filepath, sample_size, header)


def infer_parquet_schema(spark: SparkSession, filepath: str) -> StructType:
    """
    Convenience function to read schema from Parquet file.

    Args:
        spark: Active SparkSession
        filepath: Path to Parquet file

    Returns:
        StructType: Schema from Parquet

    Example:
        >>> from Helpers.SchemaInferencer import infer_parquet_schema
        >>> schema = infer_parquet_schema(spark, "cache/data.parquet")
    """
    inferencer = SchemaInferencer(spark)
    return inferencer.infer_from_parquet(filepath)
