"""
Test backwards compatibility between pandas and PySpark implementations.

This module validates that the PySpark migration maintains complete backwards
compatibility with the original pandas implementation. It ensures that:
1. Data gathering produces identical results
2. Data cleaning produces identical results
3. Statistical properties are preserved
4. Row counts and column structures match exactly
5. Numerical values match within floating-point tolerance

Task 25: Validate backwards compatibility
"""

import pytest
import pandas as pd
import os
from pathlib import Path

# Import pandas implementations (legacy)
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner

# Import PySpark implementations (new)
from Gatherer.SparkDataGatherer import SparkDataGatherer
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from Generator.Cleaners.SparkConnectedDrivingLargeDataCleaner import SparkConnectedDrivingLargeDataCleaner

# Import utilities
from Test.Utils.DataFrameComparator import DataFrameComparator
from Test.Fixtures.SparkFixtures import spark_session, spark_context
from ServiceProviders.PathProvider import PathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider


@pytest.fixture(scope="module")
def sample_data_path():
    """Provide path to sample 1k dataset for testing."""
    return str(Path(__file__).parent / "Data" / "sample_1k.csv")


@pytest.fixture(scope="module")
def temp_output_dir(tmp_path_factory):
    """Create temporary output directory for test results."""
    return tmp_path_factory.mktemp("backwards_compat_tests")


@pytest.fixture
def setup_providers(temp_output_dir):
    """Setup dependency injection providers for testing."""
    def _setup(csv_path, numrows=None):
        # Setup base path provider for Logger
        PathProvider(
            model="test_compat",
            contexts={
                "Logger.logpath": lambda model: os.path.join(temp_output_dir, "logs", "test.log"),
            }
        )

        # Setup gatherer-specific path provider
        InitialGathererPathProvider(
            model="test_compat",
            contexts={
                "DataGatherer.filepath": lambda model: csv_path,
                "DataGatherer.subsectionpath": lambda model: os.path.join(temp_output_dir, "cache.csv"),
                "DataGatherer.splitfilespath": lambda model: os.path.join(temp_output_dir, "split.csv"),
            }
        )

        # Setup context provider
        GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numrows,
            "DataGatherer.lines_per_file": 1000000,
        })

    return _setup


class TestDataGathererBackwardsCompatibility:
    """Test backwards compatibility for DataGatherer vs SparkDataGatherer."""

    def test_gather_data_row_count(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark DataGatherer produce same row count.

        This is the most fundamental test - both implementations must read
        the same number of rows from the same CSV file.
        """
        # Setup providers for both implementations
        setup_providers(csv_path=sample_data_path, numrows=1000)

        # Pandas implementation
        pandas_gatherer = DataGatherer()
        pandas_df = pandas_gatherer.gather_data()
        pandas_row_count = len(pandas_df)

        # PySpark implementation
        spark_gatherer = SparkDataGatherer()
        spark_df = spark_gatherer.gather_data()
        spark_row_count = spark_df.count()

        # Validate identical row counts
        assert pandas_row_count == spark_row_count, \
            f"Row count mismatch: pandas={pandas_row_count}, spark={spark_row_count}"
        assert pandas_row_count == 1000, \
            f"Expected 1000 rows, got {pandas_row_count}"

    def test_gather_data_columns(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark DataGatherer produce same columns.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=100
)

        # Pandas implementation
        pandas_gatherer = DataGatherer()
        pandas_df = pandas_gatherer.gather_data()
        pandas_columns = sorted(pandas_df.columns.tolist())

        # PySpark implementation
        spark_gatherer = SparkDataGatherer()
        spark_df = spark_gatherer.gather_data()
        spark_columns = sorted(spark_df.columns)

        # Validate identical columns
        assert pandas_columns == spark_columns, \
            f"Column mismatch:\n  pandas: {pandas_columns}\n  spark: {spark_columns}"

    def test_gather_data_content_equality(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark DataGatherer produce identical data content.

        This is the comprehensive validation - every cell value must match.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=100  # Use smaller sample for detailed comparison
)

        # Pandas implementation
        pandas_gatherer = DataGatherer()
        pandas_df = pandas_gatherer.gather_data()

        # PySpark implementation
        spark_gatherer = SparkDataGatherer()
        spark_df = spark_gatherer.gather_data()

        # Use DataFrameComparator for comprehensive validation
        comparator = DataFrameComparator()
        comparator.assert_pandas_spark_equal(
            pandas_df,
            spark_df,
            check_row_order=False,  # Order may differ in distributed processing
            rtol=1e-5,  # Relative tolerance for floating point
            atol=1e-8   # Absolute tolerance for floating point
        )

    def test_gather_data_dtypes_compatibility(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that data types are compatible between pandas and PySpark.

        PySpark uses different type representations than pandas, but they
        should be semantically equivalent (e.g., LongType vs int64).
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=10
)

        # Pandas implementation
        pandas_gatherer = DataGatherer()
        pandas_df = pandas_gatherer.gather_data()

        # PySpark implementation
        spark_gatherer = SparkDataGatherer()
        spark_df = spark_gatherer.gather_data()

        # Convert PySpark to pandas for type comparison
        spark_as_pandas = spark_df.toPandas()

        # Check that all columns exist in both
        pandas_cols = set(pandas_df.columns)
        spark_cols = set(spark_as_pandas.columns)
        assert pandas_cols == spark_cols, \
            f"Column set mismatch: {pandas_cols.symmetric_difference(spark_cols)}"

        # Check type compatibility for each column
        type_mismatches = []
        for col in pandas_df.columns:
            pandas_type = pandas_df[col].dtype
            spark_type = spark_as_pandas[col].dtype

            # Allow compatible types (e.g., int64/Int64, float64/Float64)
            if not _types_are_compatible(pandas_type, spark_type):
                type_mismatches.append(
                    f"{col}: pandas={pandas_type}, spark={spark_type}"
                )

        assert len(type_mismatches) == 0, \
            f"Type compatibility issues:\n" + "\n".join(type_mismatches)

    def test_gather_data_null_handling(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that null/NaN values are handled identically.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=1000
)

        # Pandas implementation
        pandas_gatherer = DataGatherer()
        pandas_df = pandas_gatherer.gather_data()
        pandas_nulls = pandas_df.isnull().sum().to_dict()

        # PySpark implementation
        spark_gatherer = SparkDataGatherer()
        spark_df = spark_gatherer.gather_data()

        # Count nulls in PySpark
        from pyspark.sql.functions import col, sum as _sum, when
        spark_nulls = {}
        for column in spark_df.columns:
            null_count = spark_df.select(
                _sum(when(col(column).isNull(), 1).otherwise(0)).alias("null_count")
            ).collect()[0]["null_count"]
            spark_nulls[column] = null_count

        # Compare null counts
        for col in pandas_df.columns:
            assert pandas_nulls[col] == spark_nulls[col], \
                f"Null count mismatch in {col}: pandas={pandas_nulls[col]}, spark={spark_nulls[col]}"


class TestCleanerBackwardsCompatibility:
    """Test backwards compatibility for ConnectedDrivingCleaner vs SparkConnectedDrivingCleaner."""

    def test_cleaner_row_preservation(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark cleaner preserve same number of rows.

        After cleaning, both implementations should filter out the same rows.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=500)

        # Get raw data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()

        # Clean with pandas
        pandas_cleaner = ConnectedDrivingCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()
        pandas_row_count = len(cleaned_pandas)

        # Clean with PySpark
        spark_cleaner = SparkConnectedDrivingCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()
        spark_row_count = cleaned_spark.count()

        # Validate identical row counts
        assert pandas_row_count == spark_row_count, \
            f"Cleaned row count mismatch: pandas={pandas_row_count}, spark={spark_row_count}"

    def test_cleaner_column_structure(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark cleaner produce same column structure.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=100)

        # Get raw data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()

        # Clean with pandas
        pandas_cleaner = ConnectedDrivingCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()
        pandas_columns = sorted(cleaned_pandas.columns.tolist())

        # Clean with PySpark
        spark_cleaner = SparkConnectedDrivingCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()
        spark_columns = sorted(cleaned_spark.columns)

        # Validate identical columns
        assert pandas_columns == spark_columns, \
            f"Column mismatch after cleaning:\n  pandas: {pandas_columns}\n  spark: {spark_columns}"

    def test_cleaner_data_equality(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark cleaner produce identical cleaned data.

        This is the comprehensive test - validates that cleaning logic is identical.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=200)

        # Get raw data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()

        # Clean with pandas
        pandas_cleaner = ConnectedDrivingCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()

        # Clean with PySpark
        spark_cleaner = SparkConnectedDrivingCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()

        # Use DataFrameComparator for comprehensive validation
        comparator = DataFrameComparator()
        comparator.assert_pandas_spark_equal(
            cleaned_pandas,
            cleaned_spark,
            check_row_order=False,
            rtol=1e-5,
            atol=1e-8
        )


class TestLargeDataCleanerBackwardsCompatibility:
    """Test backwards compatibility for ConnectedDrivingLargeDataCleaner vs SparkConnectedDrivingLargeDataCleaner."""

    def test_large_cleaner_row_preservation(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark large cleaner preserve same number of rows.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=500)

        # Get raw data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()

        # Clean with pandas
        pandas_cleaner = ConnectedDrivingLargeDataCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()
        pandas_row_count = len(cleaned_pandas)

        # Clean with PySpark
        spark_cleaner = SparkConnectedDrivingLargeDataCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()
        spark_row_count = cleaned_spark.count()

        # Validate identical row counts
        assert pandas_row_count == spark_row_count, \
            f"Large cleaned row count mismatch: pandas={pandas_row_count}, spark={spark_row_count}"

    def test_large_cleaner_data_equality(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that pandas and PySpark large cleaner produce identical cleaned data.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=300)

        # Get raw data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()

        # Clean with pandas
        pandas_cleaner = ConnectedDrivingLargeDataCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()

        # Clean with PySpark
        spark_cleaner = SparkConnectedDrivingLargeDataCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()

        # Use DataFrameComparator for comprehensive validation
        comparator = DataFrameComparator()
        comparator.assert_pandas_spark_equal(
            cleaned_pandas,
            cleaned_spark,
            check_row_order=False,
            rtol=1e-5,
            atol=1e-8
        )


class TestStatisticalEquivalence:
    """Test that statistical properties are preserved between pandas and PySpark implementations."""

    def test_statistical_properties_after_cleaning(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that statistical properties (mean, stddev, etc.) are preserved.

        Even if individual row ordering differs, statistical properties should match.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=1000)

        # Get raw data and clean
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()
        pandas_cleaner = ConnectedDrivingCleaner(sp)
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()

        spark_gatherer = SparkDataGatherer(sp, spark_session)
        raw_spark = spark_gatherer.gather_data()
        spark_cleaner = SparkConnectedDrivingCleaner(sp, spark_session)
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()

        # Convert PySpark to pandas for statistical comparison
        cleaned_spark_pandas = cleaned_spark.toPandas()

        # Select numeric columns for statistical comparison
        numeric_cols = cleaned_pandas.select_dtypes(include=['number']).columns.tolist()

        # Compare statistical properties for each numeric column
        for col in numeric_cols:
            if col in cleaned_spark_pandas.columns:
                pandas_mean = cleaned_pandas[col].mean()
                spark_mean = cleaned_spark_pandas[col].mean()

                pandas_std = cleaned_pandas[col].std()
                spark_std = cleaned_spark_pandas[col].std()

                # Allow small tolerance for floating point differences
                assert abs(pandas_mean - spark_mean) < 1e-4, \
                    f"Mean mismatch in {col}: pandas={pandas_mean}, spark={spark_mean}"

                # Standard deviation may have slightly larger tolerance
                if pd.notna(pandas_std) and pd.notna(spark_std):
                    assert abs(pandas_std - spark_std) < 1e-3, \
                        f"Std dev mismatch in {col}: pandas={pandas_std}, spark={spark_std}"

    def test_value_range_preservation(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that min/max ranges are identical between implementations.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=500)

        # Get and clean data
        pandas_gatherer = DataGatherer()
        raw_pandas = pandas_gatherer.gather_data()
        pandas_cleaner = ConnectedDrivingCleaner()
        pandas_cleaner.data = raw_pandas.copy()
        cleaned_pandas = pandas_cleaner.clean()

        spark_gatherer = SparkDataGatherer()
        raw_spark = spark_gatherer.gather_data()
        spark_cleaner = SparkConnectedDrivingCleaner()
        spark_cleaner.data = raw_spark
        cleaned_spark = spark_cleaner.clean()
        cleaned_spark_pandas = cleaned_spark.toPandas()

        # Select numeric columns
        numeric_cols = cleaned_pandas.select_dtypes(include=['number']).columns.tolist()

        # Compare min/max for each numeric column
        for col in numeric_cols:
            if col in cleaned_spark_pandas.columns:
                pandas_min = cleaned_pandas[col].min()
                spark_min = cleaned_spark_pandas[col].min()
                pandas_max = cleaned_pandas[col].max()
                spark_max = cleaned_spark_pandas[col].max()

                assert abs(pandas_min - spark_min) < 1e-6, \
                    f"Min mismatch in {col}: pandas={pandas_min}, spark={spark_min}"
                assert abs(pandas_max - spark_max) < 1e-6, \
                    f"Max mismatch in {col}: pandas={pandas_max}, spark={spark_max}"


class TestEndToEndBackwardsCompatibility:
    """End-to-end integration tests validating complete workflows."""

    def test_full_pipeline_gather_and_clean(self, spark_session, sample_data_path, setup_providers):
        """
        End-to-end test: gather → clean → validate identical results.

        This is the gold standard test - validates the complete workflow.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=500)

        # Pandas pipeline
        pandas_gatherer = DataGatherer()
        pandas_raw = pandas_gatherer.gather_data()
        pandas_cleaner = ConnectedDrivingCleaner()
        pandas_cleaner.data = pandas_raw.copy()
        pandas_final = pandas_cleaner.clean()

        # PySpark pipeline
        spark_gatherer = SparkDataGatherer()
        spark_raw = spark_gatherer.gather_data()
        spark_cleaner = SparkConnectedDrivingCleaner()
        spark_cleaner.data = spark_raw
        spark_final = spark_cleaner.clean()

        # Comprehensive validation
        comparator = DataFrameComparator()
        comparator.assert_pandas_spark_equal(
            pandas_final,
            spark_final,
            check_row_order=False,
            rtol=1e-5,
            atol=1e-8
        )

        # Additional validation: row counts must match exactly
        assert len(pandas_final) == spark_final.count(), \
            f"Final row count mismatch: pandas={len(pandas_final)}, spark={spark_final.count()}"

    def test_deterministic_processing(self, spark_session, sample_data_path, setup_providers):
        """
        Validate that processing is deterministic - running twice produces same results.
        """
        # Setup providers
        setup_providers(csv_path=sample_data_path, numrows=200)

        # Run PySpark pipeline twice
        spark_gatherer1 = SparkDataGatherer()
        spark_raw1 = spark_gatherer1.gather_data()
        spark_cleaner1 = SparkConnectedDrivingCleaner()
        spark_cleaner1.data = spark_raw1
        spark_result1 = spark_cleaner1.clean()

        spark_gatherer2 = SparkDataGatherer()
        spark_raw2 = spark_gatherer2.gather_data()
        spark_cleaner2 = SparkConnectedDrivingCleaner()
        spark_cleaner2.data = spark_raw2
        spark_result2 = spark_cleaner2.clean()

        # Validate results are identical
        comparator = DataFrameComparator()
        comparator.assert_spark_equal(
            spark_result1,
            spark_result2,
            check_row_order=False,
            rtol=1e-10,  # Very strict tolerance for determinism
            atol=1e-10
        )


# Helper functions

def _types_are_compatible(pandas_type, spark_type):
    """
    Check if pandas and PySpark types are semantically compatible.

    Args:
        pandas_type: pandas dtype
        spark_type: PySpark dtype (converted to pandas)

    Returns:
        bool: True if types are compatible
    """
    # Convert to string for comparison
    pandas_str = str(pandas_type)
    spark_str = str(spark_type)

    # Direct match
    if pandas_str == spark_str:
        return True

    # int64/Int64 compatibility
    if pandas_str in ['int64', 'Int64'] and spark_str in ['int64', 'Int64']:
        return True

    # float64/Float64 compatibility
    if pandas_str in ['float64', 'Float64'] and spark_str in ['float64', 'Float64']:
        return True

    # string/object compatibility
    if pandas_str in ['object', 'string'] and spark_str in ['object', 'string']:
        return True

    # datetime64 variations
    if 'datetime64' in pandas_str and 'datetime64' in spark_str:
        return True

    return False
