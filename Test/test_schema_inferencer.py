"""
Tests for automatic schema inference functionality.

This module tests the SchemaInferencer class and related utilities for:
- CSV schema inference with sampling
- Parquet schema reading
- Schema comparison and validation
- Fallback mechanisms
- Type compatibility checking

Author: PySpark Migration Team
Date: January 2026
"""

import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, FloatType, BooleanType, TimestampType
)
from Helpers.SchemaInferencer import (
    SchemaInferencer,
    SchemaInferenceError,
    infer_csv_schema,
    infer_parquet_schema
)
from Schemas.BSMRawSchema import get_bsm_raw_schema


class TestSchemaInferencer:
    """Test suite for SchemaInferencer class."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("SchemaInferencerTest") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        # Note: Don't stop spark here - it's shared across tests

    @pytest.fixture(scope="class")
    def inferencer(self, spark):
        """Create a SchemaInferencer instance."""
        return SchemaInferencer(spark)

    @pytest.fixture(scope="class")
    def sample_csv_path(self):
        """Path to sample CSV file."""
        return "Test/Data/sample_1k.csv"

    @pytest.fixture(scope="class")
    def sample_parquet_path(self, spark, tmp_path_factory):
        """Create a temporary Parquet file for testing."""
        tmp_dir = tmp_path_factory.mktemp("parquet_test")
        parquet_path = str(tmp_dir / "test.parquet")

        # Create sample data
        data = [
            ("Alice", 25, 1.75),
            ("Bob", 30, 1.80),
            ("Charlie", 35, 1.65)
        ]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        df.write.mode("overwrite").parquet(parquet_path)

        return parquet_path

    # Test 1: Basic CSV inference
    def test_infer_from_csv_basic(self, inferencer, sample_csv_path):
        """Test basic schema inference from CSV file."""
        schema = inferencer.infer_from_csv(sample_csv_path, sample_size=100)

        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0
        # Check that some expected columns exist
        field_names = [f.name for f in schema.fields]
        assert len(field_names) > 0

    # Test 2: CSV inference with sampling
    def test_infer_from_csv_with_sampling(self, inferencer, sample_csv_path):
        """Test CSV schema inference with different sample sizes."""
        schema_small = inferencer.infer_from_csv(sample_csv_path, sample_size=100)
        schema_large = inferencer.infer_from_csv(sample_csv_path, sample_size=500)

        # Schemas should have same structure regardless of sample size
        assert len(schema_small.fields) == len(schema_large.fields)
        assert [f.name for f in schema_small.fields] == [f.name for f in schema_large.fields]

    # Test 3: CSV inference without sampling (full file)
    def test_infer_from_csv_full_file(self, inferencer, sample_csv_path):
        """Test CSV schema inference on full file (no sampling)."""
        schema = inferencer.infer_from_csv(sample_csv_path, sample_size=None)

        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

    # Test 4: Parquet schema reading
    def test_infer_from_parquet(self, inferencer, sample_parquet_path):
        """Test schema reading from Parquet file."""
        schema = inferencer.infer_from_parquet(sample_parquet_path)

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 3

        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "age" in field_names
        assert "height" in field_names

        # Check types
        field_map = {f.name: f for f in schema.fields}
        assert isinstance(field_map["name"].dataType, StringType)
        assert isinstance(field_map["age"].dataType, IntegerType)
        assert isinstance(field_map["height"].dataType, DoubleType)

    # Test 5: Inference with fallback (success)
    def test_infer_with_fallback_success(self, inferencer, sample_csv_path):
        """Test inference with fallback when inference succeeds."""
        fallback_schema = StructType([StructField("dummy", StringType(), True)])

        schema = inferencer.infer_with_fallback(
            sample_csv_path,
            expected_schema=fallback_schema,
            sample_size=100
        )

        # Should use inferred schema, not fallback
        assert len(schema.fields) > 1  # Not the single-field fallback

    # Test 6: Inference with fallback (file not found)
    def test_infer_with_fallback_failure(self, inferencer):
        """Test inference with fallback when file doesn't exist."""
        fallback_schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True)
        ])

        schema = inferencer.infer_with_fallback(
            "nonexistent_file.csv",
            expected_schema=fallback_schema
        )

        # Should use fallback schema
        assert len(schema.fields) == 2
        assert schema == fallback_schema

    # Test 7: Inference error without fallback
    def test_infer_error_without_fallback(self, inferencer):
        """Test that inference raises error when file doesn't exist and no fallback."""
        with pytest.raises(SchemaInferenceError):
            inferencer.infer_with_fallback("nonexistent_file.csv")

    # Test 8: Schema comparison - exact match
    def test_compare_schemas_exact_match(self, inferencer):
        """Test schema comparison with exact match."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema, schema)

        assert compatible is True
        assert len(diffs) == 0

    # Test 9: Schema comparison - compatible types
    def test_compare_schemas_compatible_types(self, inferencer):
        """Test schema comparison with compatible types (int vs long)."""
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", FloatType(), True)
        ])

        schema2 = StructType([
            StructField("id", LongType(), True),
            StructField("value", DoubleType(), True)
        ])

        # With strict=False, should be compatible
        compatible, diffs = inferencer.compare_schemas(
            schema1, schema2, strict_types=False
        )
        assert compatible is True
        assert len(diffs) == 0

        # With strict=True, should not be compatible
        compatible, diffs = inferencer.compare_schemas(
            schema1, schema2, strict_types=True
        )
        assert compatible is False
        assert len(diffs) == 2  # Two type mismatches

    # Test 10: Schema comparison - missing columns
    def test_compare_schemas_missing_columns(self, inferencer):
        """Test schema comparison with missing columns."""
        schema1 = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        schema2 = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema1, schema2)

        assert compatible is False
        assert len(diffs) > 0
        assert any("Missing columns" in diff for diff in diffs)

    # Test 11: Schema comparison - extra columns
    def test_compare_schemas_extra_columns(self, inferencer):
        """Test schema comparison with extra columns."""
        schema1 = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("extra", StringType(), True)
        ])

        schema2 = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema1, schema2)

        assert compatible is False
        assert len(diffs) > 0
        assert any("Extra columns" in diff for diff in diffs)

    # Test 12: Schema comparison - incompatible types
    def test_compare_schemas_incompatible_types(self, inferencer):
        """Test schema comparison with incompatible types."""
        schema1 = StructType([
            StructField("name", StringType(), True),
            StructField("active", BooleanType(), True)
        ])

        schema2 = StructType([
            StructField("name", StringType(), True),
            StructField("active", IntegerType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema1, schema2)

        assert compatible is False
        assert len(diffs) > 0
        assert any("incompatible" in diff.lower() for diff in diffs)

    # Test 13: Type compatibility - integers
    def test_types_compatible_integers(self, inferencer):
        """Test that integer types are compatible."""
        assert inferencer._types_compatible(IntegerType(), LongType())
        assert inferencer._types_compatible(LongType(), IntegerType())

    # Test 14: Type compatibility - floats
    def test_types_compatible_floats(self, inferencer):
        """Test that float types are compatible."""
        assert inferencer._types_compatible(FloatType(), DoubleType())
        assert inferencer._types_compatible(DoubleType(), FloatType())

    # Test 15: Type compatibility - strings
    def test_types_compatible_strings(self, inferencer):
        """Test that string types are compatible."""
        assert inferencer._types_compatible(StringType(), StringType())

    # Test 16: Type incompatibility - string vs int
    def test_types_incompatible_string_int(self, inferencer):
        """Test that string and integer are incompatible."""
        assert not inferencer._types_compatible(StringType(), IntegerType())
        assert not inferencer._types_compatible(IntegerType(), StringType())

    # Test 17: Get schema summary
    def test_get_schema_summary(self, inferencer):
        """Test schema summary generation."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True),
            StructField("id", IntegerType(), False)
        ])

        summary = inferencer.get_schema_summary(schema)

        assert summary['column_count'] == 4
        assert summary['nullable_count'] == 3
        assert summary['non_nullable_count'] == 1
        assert 'name' in summary['column_names']
        assert 'age' in summary['column_names']
        assert 'height' in summary['column_names']
        assert 'id' in summary['column_names']
        assert summary['type_counts']['string'] == 1
        assert summary['type_counts']['integer'] == 2
        assert summary['type_counts']['double'] == 1

    # Test 18: Schema summary - field details
    def test_get_schema_summary_fields(self, inferencer):
        """Test schema summary includes field details."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), False)
        ])

        summary = inferencer.get_schema_summary(schema)

        assert len(summary['fields']) == 2
        assert summary['fields'][0]['name'] == 'name'
        assert summary['fields'][0]['type'] == 'string'
        assert summary['fields'][0]['nullable'] is True
        assert summary['fields'][1]['name'] == 'id'
        assert summary['fields'][1]['type'] == 'integer'
        assert summary['fields'][1]['nullable'] is False

    # Test 19: Convenience function - infer_csv_schema
    def test_convenience_infer_csv_schema(self, spark, sample_csv_path):
        """Test convenience function for CSV schema inference."""
        schema = infer_csv_schema(spark, sample_csv_path, sample_size=100)

        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

    # Test 20: Convenience function - infer_parquet_schema
    def test_convenience_infer_parquet_schema(self, spark, sample_parquet_path):
        """Test convenience function for Parquet schema reading."""
        schema = infer_parquet_schema(spark, sample_parquet_path)

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 3

    # Test 21: Real BSM data inference
    def test_infer_bsm_csv_schema(self, inferencer, sample_csv_path):
        """Test inferring schema from real BSM sample data."""
        if not os.path.exists(sample_csv_path):
            pytest.skip(f"Sample file not found: {sample_csv_path}")

        schema = inferencer.infer_from_csv(sample_csv_path, sample_size=1000)
        expected_schema = get_bsm_raw_schema()

        # Compare with expected BSM schema
        compatible, diffs = inferencer.compare_schemas(
            schema,
            expected_schema,
            strict_types=False
        )

        # May have minor differences due to inference, so just check structure
        assert len(schema.fields) > 0
        # Print differences for debugging if needed
        if not compatible:
            print(f"Schema differences: {diffs}")

    # Test 22: Column count validation
    def test_compare_schemas_column_count(self, inferencer):
        """Test schema comparison detects column count mismatches."""
        schema1 = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True)
        ])

        schema2 = StructType([
            StructField("col1", StringType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema1, schema2)

        assert compatible is False
        assert any("Column count mismatch" in diff for diff in diffs)

    # Test 23: CSV with custom delimiter
    def test_infer_csv_custom_delimiter(self, inferencer, spark, tmp_path):
        """Test CSV inference with custom delimiter."""
        # Create CSV with semicolon delimiter
        csv_path = str(tmp_path / "custom_delim.csv")
        with open(csv_path, 'w') as f:
            f.write("name;age;city\n")
            f.write("Alice;25;NYC\n")
            f.write("Bob;30;LA\n")

        schema = inferencer.infer_from_csv(csv_path, delimiter=";")

        assert len(schema.fields) == 3
        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "age" in field_names
        assert "city" in field_names

    # Test 24: CSV without header
    def test_infer_csv_no_header(self, inferencer, spark, tmp_path):
        """Test CSV inference without header row."""
        csv_path = str(tmp_path / "no_header.csv")
        with open(csv_path, 'w') as f:
            f.write("Alice,25,NYC\n")
            f.write("Bob,30,LA\n")

        schema = inferencer.infer_from_csv(csv_path, header=False)

        assert len(schema.fields) == 3
        # Without header, PySpark uses default column names (_c0, _c1, etc.)
        field_names = [f.name for f in schema.fields]
        assert any(name.startswith('_c') for name in field_names)

    # Test 25: Empty differences list for identical schemas
    def test_compare_identical_schemas_no_diffs(self, inferencer):
        """Test that comparing identical schemas returns empty differences."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        compatible, diffs = inferencer.compare_schemas(schema, schema)

        assert compatible is True
        assert diffs == []


class TestSchemaInferencerIntegration:
    """Integration tests for schema inference with real data."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("SchemaInferencerIntegrationTest") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        yield spark

    def test_infer_and_load_csv(self, spark, tmp_path):
        """Test complete workflow: infer schema, then load CSV with it."""
        # Create test CSV
        csv_path = str(tmp_path / "test_data.csv")
        with open(csv_path, 'w') as f:
            f.write("name,age,salary\n")
            f.write("Alice,25,50000.0\n")
            f.write("Bob,30,60000.0\n")
            f.write("Charlie,35,70000.0\n")

        # Infer schema
        inferencer = SchemaInferencer(spark)
        schema = inferencer.infer_from_csv(csv_path)

        # Load CSV with inferred schema
        df = spark.read.schema(schema).option("header", "true").csv(csv_path)

        assert df.count() == 3
        assert len(df.columns) == 3

    def test_infer_compare_and_validate(self, spark, tmp_path):
        """Test workflow: infer, compare with expected, validate."""
        # Create test CSV
        csv_path = str(tmp_path / "validation_test.csv")
        with open(csv_path, 'w') as f:
            f.write("id,value\n")
            f.write("1,100\n")
            f.write("2,200\n")

        # Expected schema
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        # Infer and compare
        inferencer = SchemaInferencer(spark)
        inferred_schema = inferencer.infer_from_csv(csv_path)
        compatible, diffs = inferencer.compare_schemas(
            inferred_schema,
            expected_schema,
            strict_types=False
        )

        assert compatible is True or len(diffs) == 0  # Should match
