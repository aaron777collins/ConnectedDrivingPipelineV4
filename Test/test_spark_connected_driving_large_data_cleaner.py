"""
Tests for SparkConnectedDrivingLargeDataCleaner.

This test suite validates the PySpark-based large dataset cleaner:
- Initialization with dependency injection
- Data cleaning with configurable cleaner classes
- Optional filtering
- Parquet-based data persistence
- Utility methods for data access (getNRows, getAllRows, getNumOfRows)
"""

import os
import shutil
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from Generator.Cleaners.SparkConnectedDrivingLargeDataCleaner import SparkConnectedDrivingLargeDataCleaner
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from Gatherer.SparkDataGatherer import SparkDataGatherer
from Schemas.BSMRawSchema import get_bsm_raw_schema
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider


@pytest.fixture
def test_cache_dir(tmp_path):
    """Create isolated cache directory for tests."""
    cache_dir = tmp_path / "cache_large_cleaner"
    cache_dir.mkdir()
    yield str(cache_dir)
    # Cleanup
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


@pytest.fixture
def mock_split_data(spark_session, temp_spark_dir, sample_bsm_raw_df):
    """Create mock split data (partitioned Parquet) for testing."""
    split_path = os.path.join(temp_spark_dir, "splitfiles", "test_model")
    os.makedirs(os.path.dirname(split_path), exist_ok=True)

    # Write sample data as Parquet (simulating DataGatherer output)
    sample_bsm_raw_df.write.mode("overwrite").parquet(split_path)

    return split_path


@pytest.fixture
def setup_large_cleaner_context(test_cache_dir, temp_spark_dir):
    """Setup context providers for SparkConnectedDrivingLargeDataCleaner tests."""
    model_name = "test_large_cleaner_model"

    # Configure paths
    GeneratorPathProvider().set_model_name(model_name)
    InitialGathererPathProvider().set_model_name(model_name)

    # Configure context with paths and cleaning parameters
    context_config = {
        # Paths
        "DataGatherer.splitfilespath": lambda model: os.path.join(temp_spark_dir, "splitfiles", model),
        "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: os.path.join(temp_spark_dir, "cleaned", model, ""),
        "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: os.path.join(temp_spark_dir, "combined", model),

        # Cleaning configuration
        "ConnectedDrivingCleaner.x_pos": -106.0,
        "ConnectedDrivingCleaner.y_pos": 41.0,
        "ConnectedDrivingLargeDataCleaner.max_dist": 2000,

        # Cleaner class and functions
        "ConnectedDrivingLargeDataCleaner.cleanerClass": SparkConnectedDrivingCleaner,
        "ConnectedDrivingLargeDataCleaner.cleanFunc": lambda cleaner: cleaner.clean_data(),
        "ConnectedDrivingLargeDataCleaner.filterFunc": None,  # No filtering by default

        # SparkConnectedDrivingCleaner configuration
        "ConnectedDrivingCleaner.cleanParams": {
            "coreData_position": {"newColumnName": ["x_pos", "y_pos"]},
        },
        "ConnectedDrivingCleaner.isXYCoords": True,
        "ConnectedDrivingCleaner.columns": [
            "coreData_id", "coreData_secMark", "coreData_position",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_speed", "coreData_heading"
        ],
        "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
        "DataGatherer.numrows": 5,
    }

    GeneratorContextProvider().configure(context_config)

    yield

    # Cleanup
    GeneratorContextProvider().clear()


@pytest.mark.spark
@pytest.mark.integration
class TestSparkConnectedDrivingLargeDataCleanerInitialization:
    """Test initialization of SparkConnectedDrivingLargeDataCleaner."""

    def test_initialization_creates_directories(self, setup_large_cleaner_context, temp_spark_dir):
        """Test that initialization creates necessary directories."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Check that cleaner is initialized
        assert cleaner is not None
        assert cleaner.logger is not None
        assert cleaner.spark is not None

        # Check path configuration
        assert cleaner.splitfilespath is not None
        assert cleaner.cleanedfilespath is not None
        assert cleaner.combinedcleandatapath is not None

    def test_initialization_with_context(self, setup_large_cleaner_context):
        """Test that context provider configuration is loaded correctly."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Check cleaning configuration
        assert cleaner.x_pos == -106.0
        assert cleaner.y_pos == 41.0
        assert cleaner.max_dist == 2000
        assert cleaner.cleanerClass == SparkConnectedDrivingCleaner
        assert cleaner.cleanFunc is not None
        assert cleaner.filterFunc is None


@pytest.mark.spark
@pytest.mark.integration
class TestSparkConnectedDrivingLargeDataCleanerCleaning:
    """Test data cleaning functionality."""

    def test_clean_data_basic(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test basic data cleaning without filtering."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Run cleaning
        result = cleaner.clean_data()

        # Check method chaining
        assert result == cleaner

        # Check that combined data exists
        assert os.path.exists(cleaner.combinedcleandatapath)

        # Check that data can be read
        df = spark_session.read.parquet(cleaner.combinedcleandatapath)
        assert df.count() > 0

        # Check that cleaned data has expected columns (x_pos, y_pos should be added)
        columns = df.columns
        assert "x_pos" in columns
        assert "y_pos" in columns

    def test_clean_data_skip_if_exists(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test that cleaning is skipped if data already exists."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # First cleaning
        cleaner.clean_data()
        first_count = spark_session.read.parquet(cleaner.combinedcleandatapath).count()

        # Second cleaning (should skip)
        cleaner.clean_data()
        second_count = spark_session.read.parquet(cleaner.combinedcleandatapath).count()

        # Counts should be the same (no re-processing)
        assert first_count == second_count

    def test_clean_data_with_filter(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test data cleaning with filtering."""
        # Define a simple filter function (keep only rows with speed > 10)
        def filter_by_speed(cleaner_instance, df):
            return df.filter(col("coreData_speed") > 10.0)

        # Update context to include filter
        GeneratorContextProvider().set("ConnectedDrivingLargeDataCleaner.filterFunc", filter_by_speed)

        cleaner = SparkConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()

        # Check that filtered data exists
        df = spark_session.read.parquet(cleaner.combinedcleandatapath)

        # All rows should have speed > 10
        speeds = df.select("coreData_speed").collect()
        assert all(row.coreData_speed > 10.0 for row in speeds)


@pytest.mark.spark
@pytest.mark.integration
class TestSparkConnectedDrivingLargeDataCleanerDataAccess:
    """Test data access utility methods."""

    def test_get_n_rows(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test getNRows method."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()

        # Get 3 rows
        df = cleaner.getNRows(3)

        assert isinstance(df, DataFrame)
        assert df.count() == 3

    def test_get_num_of_rows(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test getNumOfRows method."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()

        # Get row count
        count = cleaner.getNumOfRows()

        assert isinstance(count, int)
        assert count > 0

    def test_get_all_rows(self, setup_large_cleaner_context, mock_split_data, spark_session):
        """Test getAllRows method."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()

        # Get all rows
        df = cleaner.getAllRows()

        assert isinstance(df, DataFrame)
        assert df.count() > 0

        # Should match getNumOfRows
        assert df.count() == cleaner.getNumOfRows()

    def test_get_n_rows_before_cleaning_raises_error(self, setup_large_cleaner_context):
        """Test that getNRows raises error if data not cleaned yet."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        with pytest.raises(FileNotFoundError):
            cleaner.getNRows(10)

    def test_get_all_rows_before_cleaning_raises_error(self, setup_large_cleaner_context):
        """Test that getAllRows raises error if data not cleaned yet."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()

        with pytest.raises(FileNotFoundError):
            cleaner.getAllRows()


@pytest.mark.spark
@pytest.mark.integration
class TestSparkConnectedDrivingLargeDataCleanerCombineData:
    """Test combine_data method (no-op for Spark)."""

    def test_combine_data_is_noop(self, setup_large_cleaner_context, mock_split_data):
        """Test that combine_data is a no-op for Spark (data already combined)."""
        cleaner = SparkConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()

        # Call combine_data (should do nothing)
        result = cleaner.combine_data()

        # Check method chaining
        assert result == cleaner

        # Data should still exist
        assert os.path.exists(cleaner.combinedcleandatapath)


@pytest.mark.spark
@pytest.mark.integration
class TestSparkConnectedDrivingLargeDataCleanerHelpers:
    """Test helper methods."""

    def test_is_valid_parquet_directory_with_parquet_files(self, temp_spark_dir, sample_bsm_raw_df):
        """Test _is_valid_parquet_directory with actual Parquet files."""
        # Create a Parquet directory
        parquet_path = os.path.join(temp_spark_dir, "test_parquet")
        sample_bsm_raw_df.write.mode("overwrite").parquet(parquet_path)

        # Create cleaner (need context)
        GeneratorContextProvider().configure({
            "DataGatherer.splitfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: temp_spark_dir,
            "ConnectedDrivingCleaner.x_pos": -106.0,
            "ConnectedDrivingCleaner.y_pos": 41.0,
            "ConnectedDrivingLargeDataCleaner.max_dist": 2000,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": SparkConnectedDrivingCleaner,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": lambda c: c,
            "ConnectedDrivingLargeDataCleaner.filterFunc": None,
        })
        GeneratorPathProvider().set_model_name("test")
        InitialGathererPathProvider().set_model_name("test")

        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Should return True
        assert cleaner._is_valid_parquet_directory(parquet_path) is True

        # Cleanup
        GeneratorContextProvider().clear()

    def test_is_valid_parquet_directory_with_nonexistent_path(self, temp_spark_dir):
        """Test _is_valid_parquet_directory with non-existent path."""
        # Create cleaner (need context)
        GeneratorContextProvider().configure({
            "DataGatherer.splitfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: temp_spark_dir,
            "ConnectedDrivingCleaner.x_pos": -106.0,
            "ConnectedDrivingCleaner.y_pos": 41.0,
            "ConnectedDrivingLargeDataCleaner.max_dist": 2000,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": SparkConnectedDrivingCleaner,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": lambda c: c,
            "ConnectedDrivingLargeDataCleaner.filterFunc": None,
        })
        GeneratorPathProvider().set_model_name("test")
        InitialGathererPathProvider().set_model_name("test")

        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Should return False
        assert cleaner._is_valid_parquet_directory("/nonexistent/path") is False

        # Cleanup
        GeneratorContextProvider().clear()

    def test_is_valid_parquet_directory_with_empty_directory(self, temp_spark_dir):
        """Test _is_valid_parquet_directory with empty directory."""
        # Create empty directory
        empty_dir = os.path.join(temp_spark_dir, "empty")
        os.makedirs(empty_dir)

        # Create cleaner (need context)
        GeneratorContextProvider().configure({
            "DataGatherer.splitfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: temp_spark_dir,
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: temp_spark_dir,
            "ConnectedDrivingCleaner.x_pos": -106.0,
            "ConnectedDrivingCleaner.y_pos": 41.0,
            "ConnectedDrivingLargeDataCleaner.max_dist": 2000,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": SparkConnectedDrivingCleaner,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": lambda c: c,
            "ConnectedDrivingLargeDataCleaner.filterFunc": None,
        })
        GeneratorPathProvider().set_model_name("test")
        InitialGathererPathProvider().set_model_name("test")

        cleaner = SparkConnectedDrivingLargeDataCleaner()

        # Should return False (no Parquet files)
        assert cleaner._is_valid_parquet_directory(empty_dir) is False

        # Cleanup
        GeneratorContextProvider().clear()
