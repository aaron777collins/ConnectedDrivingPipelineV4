"""
Tests for ParquetCache decorator.

This test suite validates the ParquetCache decorator which provides
MD5-based caching for PySpark DataFrames using Parquet format.
"""

import pytest
import os
import shutil
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from Decorators.ParquetCache import ParquetCache
from Helpers.SparkSessionManager import SparkSessionManager


@pytest.fixture
def spark():
    """Get Spark session for testing."""
    return SparkSessionManager.get_session(app_name="TestParquetCache")


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    yield str(cache_dir)
    # Cleanup after test
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


@pytest.fixture(autouse=True)
def cleanup_default_cache():
    """Clean up default cache directory before and after each test."""
    default_cache = "cache/NO_MODEL_PROVIDED"
    # Cleanup before test
    if os.path.exists(default_cache):
        shutil.rmtree(default_cache)
    yield
    # Cleanup after test
    if os.path.exists(default_cache):
        shutil.rmtree(default_cache)


@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    data = [
        (1, "Alice", 100.5),
        (2, "Bob", 200.7),
        (3, "Charlie", 300.9)
    ]

    return spark.createDataFrame(data, schema)


class TestParquetCacheBasic:
    """Basic ParquetCache functionality tests."""

    def test_cache_basic_functionality(self, spark, temp_cache_dir, sample_dataframe):
        """Test that ParquetCache caches and retrieves DataFrames correctly."""

        # Use a list to track calls (mutable so it works in nested scope)
        call_tracker = [0]

        @ParquetCache
        def expensive_operation(param1, param2) -> DataFrame:
            call_tracker[0] += 1
            # Simulate expensive operation
            return sample_dataframe

        # First call - should execute function
        result1 = expensive_operation("test", "params")
        assert call_tracker[0] == 1
        assert isinstance(result1, DataFrame)
        assert result1.count() == 3

        # Second call with same params - should use cache
        result2 = expensive_operation("test", "params")
        assert call_tracker[0] == 1  # Should NOT increment (cached)
        assert isinstance(result2, DataFrame)
        assert result2.count() == 3

        # Third call with different params - should execute function
        result3 = expensive_operation("different", "params")
        assert call_tracker[0] == 2  # Should increment (different params)
        assert isinstance(result3, DataFrame)
        assert result3.count() == 3


    def test_cached_data_integrity(self, spark, temp_cache_dir, sample_dataframe):
        """Test that cached data is identical to original data."""

        @ParquetCache
        def create_dataframe() -> DataFrame:
            return sample_dataframe

        # First call - creates cache
        original = create_dataframe()
        original_rows = sorted(original.collect(), key=lambda r: r.id)

        # Second call - reads from cache
        cached = create_dataframe()
        cached_rows = sorted(cached.collect(), key=lambda r: r.id)

        # Compare row counts
        assert len(original_rows) == len(cached_rows)

        # Compare data (sorted by id to ensure consistent ordering)
        for orig_row, cached_row in zip(original_rows, cached_rows):
            assert orig_row == cached_row


    def test_cache_with_explicit_path(self, spark, temp_cache_dir, sample_dataframe):
        """Test ParquetCache with explicit cache path override."""

        @ParquetCache
        def create_dataframe() -> DataFrame:
            return sample_dataframe

        explicit_path = os.path.join(temp_cache_dir, "explicit_cache.parquet")

        # Call with explicit cache path
        result1 = create_dataframe(full_file_cache_path=explicit_path)
        assert result1.count() == 3

        # Verify cache file exists
        assert os.path.exists(explicit_path)

        # Call again with same explicit path - should use cache
        result2 = create_dataframe(full_file_cache_path=explicit_path)
        assert result2.count() == 3


class TestParquetCacheWithCacheVariables:
    """Test ParquetCache with cache_variables parameter."""

    def test_cache_variables_parameter(self, spark, temp_cache_dir, sample_dataframe):
        """Test that cache_variables controls cache key generation."""
        call_tracker = [0]

        @ParquetCache
        def process_data(param1, param2, ignore_this) -> DataFrame:
            call_tracker[0] += 1
            return sample_dataframe

        # First call
        result1 = process_data("a", "b", "ignored1", cache_variables=["a", "b"])
        assert call_tracker[0] == 1

        # Second call with different ignore_this but same cache_variables
        result2 = process_data("a", "b", "ignored2", cache_variables=["a", "b"])
        assert call_tracker[0] == 1  # Should use cache (ignore_this not in cache_variables)

        # Third call with different param1
        result3 = process_data("c", "b", "ignored3", cache_variables=["c", "b"])
        assert call_tracker[0] == 2  # Should execute function (param1 changed)


class TestParquetCacheSchema:
    """Test ParquetCache with different schemas."""

    def test_cache_preserves_schema(self, spark, temp_cache_dir):
        """Test that ParquetCache preserves DataFrame schema.

        Note: Parquet format may change nullable=False to nullable=True
        for non-primitive types. This is a known Parquet limitation.
        """

        schema = StructType([
            StructField("str_col", StringType(), False),
            StructField("int_col", IntegerType(), False),
            StructField("double_col", DoubleType(), True)
        ])

        data = [("test", 42, 3.14)]

        @ParquetCache
        def create_typed_dataframe() -> DataFrame:
            return spark.createDataFrame(data, schema)

        # First call
        original = create_typed_dataframe()
        original_schema = original.schema

        # Second call (from cache)
        cached = create_typed_dataframe()
        cached_schema = cached.schema

        # Compare schema field names and types (not nullable since Parquet may change it)
        assert len(original_schema.fields) == len(cached_schema.fields)

        for orig_field, cached_field in zip(original_schema.fields, cached_schema.fields):
            assert orig_field.name == cached_field.name
            assert orig_field.dataType == cached_field.dataType
            # Note: nullable may differ after Parquet round-trip


    def test_cache_with_complex_types(self, spark, temp_cache_dir):
        """Test ParquetCache with complex data types."""
        from pyspark.sql.types import ArrayType, MapType

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])

        data = [
            (1, ["tag1", "tag2"], {"key1": "value1"}),
            (2, ["tag3"], {"key2": "value2"})
        ]

        @ParquetCache
        def create_complex_dataframe() -> DataFrame:
            return spark.createDataFrame(data, schema)

        # First call
        original = create_complex_dataframe()

        # Second call (from cache)
        cached = create_complex_dataframe()

        # Compare data
        assert original.count() == cached.count()
        assert original.schema == cached.schema


class TestParquetCacheEdgeCases:
    """Test ParquetCache edge cases and error handling."""

    def test_cache_empty_dataframe(self, spark, temp_cache_dir):
        """Test caching an empty DataFrame."""

        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True)
        ])

        @ParquetCache
        def create_empty_dataframe() -> DataFrame:
            return spark.createDataFrame([], schema)

        # First call
        original = create_empty_dataframe()
        assert original.count() == 0

        # Second call (from cache)
        cached = create_empty_dataframe()
        assert cached.count() == 0
        assert cached.schema == original.schema


    def test_cache_large_dataframe(self, spark, temp_cache_dir):
        """Test caching a larger DataFrame to verify Parquet efficiency."""

        @ParquetCache
        def create_large_dataframe() -> DataFrame:
            # Create a DataFrame with 10,000 rows
            data = [(i, f"name_{i}", float(i) * 1.5) for i in range(10000)]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", DoubleType(), True)
            ])
            return spark.createDataFrame(data, schema)

        # First call
        original = create_large_dataframe()
        assert original.count() == 10000

        # Second call (from cache)
        cached = create_large_dataframe()
        assert cached.count() == 10000

        # Verify cache directory exists (default is cache/NO_MODEL_PROVIDED/)
        default_cache_dir = "cache/NO_MODEL_PROVIDED"
        assert os.path.exists(default_cache_dir)
        cache_entries = os.listdir(default_cache_dir)
        assert len(cache_entries) > 0


class TestParquetCacheIntegration:
    """Integration tests for ParquetCache with real-world scenarios."""

    def test_cache_with_transformations(self, spark, temp_cache_dir):
        """Test caching DataFrames after transformations."""

        @ParquetCache
        def process_and_filter(min_value) -> DataFrame:
            # Create initial data
            data = [(i, float(i) * 2) for i in range(100)]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", DoubleType(), True)
            ])
            df = spark.createDataFrame(data, schema)

            # Apply transformations
            from pyspark.sql.functions import col
            filtered = df.filter(col("value") >= min_value)

            return filtered

        # First call with min_value=50
        result1 = process_and_filter(50.0)
        count1 = result1.count()
        assert count1 > 0

        # Second call with same min_value (should use cache)
        result2 = process_and_filter(50.0)
        count2 = result2.count()
        assert count2 == count1

        # Third call with different min_value (should recompute)
        result3 = process_and_filter(100.0)
        count3 = result3.count()
        assert count3 < count1  # Should have fewer rows


    def test_multiple_cached_functions(self, spark, temp_cache_dir):
        """Test multiple functions using ParquetCache simultaneously."""

        @ParquetCache
        def function_a() -> DataFrame:
            data = [(1, "a"), (2, "b")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("letter", StringType(), True)
            ])
            return spark.createDataFrame(data, schema)

        @ParquetCache
        def function_b() -> DataFrame:
            data = [(10, "x"), (20, "y")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("letter", StringType(), True)
            ])
            return spark.createDataFrame(data, schema)

        # Call both functions
        result_a = function_a()
        result_b = function_b()

        assert result_a.count() == 2
        assert result_b.count() == 2

        # Verify different cache files exist (default cache/NO_MODEL_PROVIDED/)
        default_cache_dir = "cache/NO_MODEL_PROVIDED"
        assert os.path.exists(default_cache_dir)
        cache_entries = os.listdir(default_cache_dir)
        assert len(cache_entries) >= 2  # At least 2 cache files (one per function)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
