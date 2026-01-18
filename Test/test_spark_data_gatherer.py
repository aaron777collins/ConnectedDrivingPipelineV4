"""
Tests for SparkDataGatherer class.

Validates PySpark-based data gathering functionality including:
- CSV reading with schema validation
- Row limiting
- Parquet caching via ParquetCache decorator
- Large dataset splitting/partitioning
"""

import pytest
import os
import tempfile
import shutil
from pyspark.sql import DataFrame as SparkDataFrame

from Gatherer.SparkDataGatherer import SparkDataGatherer
from Schemas.BSMRawSchema import get_bsm_raw_schema, get_bsm_raw_column_names
from ServiceProviders.PathProvider import PathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from Decorators.StandardDependencyInjection import StandardDependencyInjection


@pytest.fixture
def temp_data_dir(temp_spark_dir):
    """Create temporary directory for test data."""
    return temp_spark_dir


@pytest.fixture
def setup_providers(temp_data_dir):
    """Setup dependency injection providers for testing."""
    def _setup(csv_path=None, cache_path=None, split_path=None, numrows=None, lines_per_file=1000):
        # Setup base path provider for Logger
        PathProvider(
            model="test",
            contexts={
                "Logger.logpath": lambda model: os.path.join(temp_data_dir, "logs", "test.log"),
            }
        )

        # Setup gatherer-specific path provider
        InitialGathererPathProvider(
            model="test",
            contexts={
                "DataGatherer.filepath": lambda model: csv_path or os.path.join(temp_data_dir, "data.csv"),
                "DataGatherer.subsectionpath": lambda model: cache_path or os.path.join(temp_data_dir, "cache.csv"),
                "DataGatherer.splitfilespath": lambda model: split_path or os.path.join(temp_data_dir, "split.csv"),
            }
        )

        # Setup context provider
        GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numrows,
            "DataGatherer.lines_per_file": lines_per_file,
        })

    return _setup


@pytest.fixture
def sample_csv_file(temp_data_dir, sample_bsm_raw_df, spark_session):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_data_dir, "test_data.csv")

    # Convert Spark DataFrame to pandas and write as CSV
    sample_bsm_raw_df.toPandas().to_csv(csv_path, index=False)

    return csv_path


@pytest.mark.spark
@pytest.mark.integration
class TestSparkDataGathererBasic:
    """Basic functionality tests for SparkDataGatherer."""

    def test_initialization(self, setup_providers, temp_data_dir):
        """Test SparkDataGatherer initialization."""
        # Setup providers with test configuration
        setup_providers(numrows=100, lines_per_file=1000)

        # Create gatherer
        gatherer = SparkDataGatherer()

        # Verify initialization
        assert gatherer.spark is not None
        assert gatherer.data is None
        assert gatherer.numrows == 100
        assert gatherer.filepath.endswith("data.csv")
        # Verify CSV path converted to Parquet
        assert gatherer.subsectionpath.endswith("cache.parquet")

    def test_csv_path_conversion(self, setup_providers, temp_data_dir):
        """Test that CSV cache paths are converted to Parquet."""
        setup_providers()

        gatherer = SparkDataGatherer()

        # Verify CSV extensions converted to Parquet (end of path)
        assert gatherer.subsectionpath.endswith("cache.parquet")
        assert not gatherer.subsectionpath.endswith(".csv")


@pytest.mark.spark
@pytest.mark.integration
class TestSparkDataGathererReading:
    """Tests for data reading functionality."""

    def test_gather_data_from_csv(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test gathering data from CSV file."""
        setup_providers(csv_path=sample_csv_file)

        # Create gatherer and read data
        gatherer = SparkDataGatherer()
        data = gatherer.gather_data()

        # Verify data loaded
        assert isinstance(data, SparkDataFrame)
        assert data.count() == 5  # Sample data has 5 rows

        # Verify schema matches BSM raw schema
        expected_cols = get_bsm_raw_column_names()
        actual_cols = data.columns
        assert set(actual_cols) == set(expected_cols)

    def test_gather_data_with_row_limit(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test gathering data with row limit."""
        setup_providers(csv_path=sample_csv_file, numrows=2)

        gatherer = SparkDataGatherer()
        data = gatherer.gather_data()

        # Verify row limit applied
        assert data.count() == 2

    def test_get_gathered_data(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test get_gathered_data method."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = SparkDataGatherer()

        # Initially should be None
        assert gatherer.get_gathered_data() is None

        # After gathering, should return data
        gatherer.gather_data()
        result = gatherer.get_gathered_data()
        assert isinstance(result, SparkDataFrame)
        assert result.count() == 5


@pytest.mark.spark
@pytest.mark.cache
class TestSparkDataGathererCaching:
    """Tests for Parquet caching functionality."""

    def test_parquet_cache_integration(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test that ParquetCache decorator works with gather_data."""
        cache_path = os.path.join(temp_data_dir, "cache.parquet")

        setup_providers(csv_path=sample_csv_file, cache_path=cache_path)

        # First call - should read from CSV and create cache
        gatherer1 = SparkDataGatherer()
        data1 = gatherer1.gather_data()

        # Verify cache file created
        assert os.path.exists(cache_path)

        # Second call - should read from cache
        gatherer2 = SparkDataGatherer()
        data2 = gatherer2.gather_data()

        # Verify data matches
        assert data1.count() == data2.count()
        assert data1.columns == data2.columns


@pytest.mark.spark
@pytest.mark.integration
class TestSparkDataGathererSplitting:
    """Tests for large dataset splitting functionality."""

    def test_split_large_data(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test splitting large dataset into partitioned Parquet."""
        split_path = os.path.join(temp_data_dir, "split.parquet")

        setup_providers(csv_path=sample_csv_file, lines_per_file=2)

        gatherer = SparkDataGatherer()

        # Gather data first
        gatherer.gather_data()

        # Split data
        result = gatherer.split_large_data()

        # Verify return value for chaining
        assert result is gatherer

        # Verify partitioned Parquet created
        assert os.path.isdir(split_path)

        # Verify can read partitioned data
        split_df = spark_session.read.parquet(split_path)
        assert split_df.count() == 5  # Original row count preserved

    def test_split_large_data_skip_if_exists(self, setup_providers, sample_csv_file, temp_data_dir, spark_session):
        """Test that split_large_data skips if partitioned files exist."""
        split_path = os.path.join(temp_data_dir, "split.parquet")

        setup_providers(csv_path=sample_csv_file, lines_per_file=2)

        # Create gatherer and split once
        gatherer1 = SparkDataGatherer()
        gatherer1.gather_data()
        gatherer1.split_large_data()

        # Get modification time of first split
        first_mtime = os.path.getmtime(split_path)

        # Create new gatherer and split again (should skip)
        gatherer2 = SparkDataGatherer()
        gatherer2.gather_data()
        gatherer2.split_large_data()

        # Verify files not recreated (same modification time)
        second_mtime = os.path.getmtime(split_path)
        assert first_mtime == second_mtime


@pytest.mark.spark
@pytest.mark.integration
class TestSparkDataGathererWithRealData:
    """Integration tests using real sample datasets."""

    @pytest.mark.parametrize("dataset_size", ["1k", "10k"])
    def test_gather_from_sample_datasets(self, setup_providers, dataset_size, temp_data_dir, spark_session):
        """Test gathering data from actual sample datasets."""
        # Get sample dataset path
        dataset_path = f"Test/Data/sample_{dataset_size}.csv"

        if not os.path.exists(dataset_path):
            pytest.skip(f"Sample dataset {dataset_path} not found")

        setup_providers(
            csv_path=dataset_path,
            cache_path=os.path.join(temp_data_dir, f"cache_{dataset_size}.parquet"),
            split_path=os.path.join(temp_data_dir, f"split_{dataset_size}")
        )

        gatherer = SparkDataGatherer()
        data = gatherer.gather_data()

        # Verify expected row count
        expected_counts = {"1k": 1000, "10k": 10000}
        assert data.count() == expected_counts[dataset_size]

        # Verify schema
        assert set(data.columns) == set(get_bsm_raw_column_names())
