"""
Tests for DaskDataGatherer class.

Validates Dask-based data gathering functionality including:
- CSV reading with schema validation
- Row limiting with .head()
- Parquet caching via DaskParquetCache decorator
- Large dataset splitting/partitioning
- Memory management and persistence
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import dask.dataframe as dd

from Gatherer.DaskDataGatherer import DaskDataGatherer
from Schemas.BSMRawSchema import get_bsm_raw_schema, get_bsm_raw_column_names
from ServiceProviders.PathProvider import PathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from Decorators.StandardDependencyInjection import StandardDependencyInjection


@pytest.fixture
def temp_data_dir(temp_dask_dir):
    """Create temporary directory for test data."""
    return temp_dask_dir


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
def sample_csv_file(temp_data_dir, sample_bsm_raw_dask_df, dask_client):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_data_dir, "test_data.csv")

    # Convert Dask DataFrame to pandas and write as CSV
    sample_bsm_raw_dask_df.compute().to_csv(csv_path, index=False)

    return csv_path


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererBasic:
    """Basic functionality tests for DaskDataGatherer."""

    def test_initialization(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test DaskDataGatherer initialization."""
        # Setup providers with test configuration
        setup_providers(csv_path=sample_csv_file, numrows=100, lines_per_file=1000)

        # Create gatherer
        gatherer = DaskDataGatherer()

        # Verify initialization
        assert gatherer.client is not None
        assert gatherer.data is None
        assert gatherer.numrows == 100
        assert gatherer.filepath == sample_csv_file
        # Verify CSV path converted to Parquet
        assert gatherer.subsectionpath.endswith("cache.parquet")

    def test_csv_path_conversion(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test that CSV cache paths are converted to Parquet."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()

        # Verify CSV extensions converted to Parquet (end of path)
        assert gatherer.subsectionpath.endswith("cache.parquet")
        assert not gatherer.subsectionpath.endswith(".csv")

    def test_blocksize_configuration(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test blocksize configuration from context provider."""
        setup_providers(csv_path=sample_csv_file)

        # Create gatherer with default blocksize
        gatherer = DaskDataGatherer()
        assert gatherer.blocksize == '128MB'

    def test_assume_missing_configuration(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test assume_missing configuration from context provider."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()
        assert gatherer.assume_missing is True


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererReading:
    """Tests for data reading functionality."""

    def test_gather_data_from_csv(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test gathering data from CSV file."""
        setup_providers(csv_path=sample_csv_file)

        # Create gatherer and read data
        gatherer = DaskDataGatherer()
        data = gatherer.gather_data()

        # Verify data loaded
        assert isinstance(data, dd.DataFrame)
        # Compute to verify row count
        assert len(data.compute()) == 5  # Sample data has 5 rows

        # Verify data has key BSM columns (fixture doesn't have full schema)
        assert "metadata_generatedAt" in data.columns
        assert "coreData_id" in data.columns
        assert "coreData_position_lat" in data.columns
        assert "coreData_speed" in data.columns

    def test_gather_data_with_row_limit(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test gathering data with row limit."""
        setup_providers(csv_path=sample_csv_file, numrows=2)

        gatherer = DaskDataGatherer()
        data = gatherer.gather_data()

        # Verify row limit applied
        assert len(data.compute()) == 2

    def test_get_gathered_data(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test get_gathered_data method."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()

        # Initially should be None
        assert gatherer.get_gathered_data() is None

        # After gathering, should return data
        gatherer.gather_data()
        result = gatherer.get_gathered_data()
        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == 5

    def test_compute_data(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test compute_data method converts to pandas."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()

        # Compute to pandas
        pandas_df = gatherer.compute_data()

        # Verify pandas DataFrame
        assert isinstance(pandas_df, pd.DataFrame)
        assert len(pandas_df) == 5

    def test_compute_data_without_gather_raises_error(self, setup_providers, temp_data_dir, dask_client):
        """Test compute_data raises error if data not gathered."""
        setup_providers()

        gatherer = DaskDataGatherer()

        # Should raise ValueError
        with pytest.raises(ValueError, match="No data gathered yet"):
            gatherer.compute_data()


@pytest.mark.dask
@pytest.mark.cache
class TestDaskDataGathererCaching:
    """Tests for Parquet caching functionality."""

    def test_parquet_cache_integration(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test that DaskParquetCache decorator works with gather_data."""
        cache_path = os.path.join(temp_data_dir, "cache.parquet")

        setup_providers(csv_path=sample_csv_file, cache_path=cache_path)

        # First call - should read from CSV and create cache
        gatherer1 = DaskDataGatherer()
        data1 = gatherer1.gather_data()

        # Verify cache directory created (Parquet is a directory)
        assert os.path.exists(cache_path)
        assert os.path.isdir(cache_path)

        # Second call - should read from cache
        gatherer2 = DaskDataGatherer()
        data2 = gatherer2.gather_data()

        # Verify data matches
        df1 = data1.compute()
        df2 = data2.compute()
        assert len(df1) == len(df2)
        assert df1.columns.tolist() == df2.columns.tolist()

    def test_parquet_cache_with_row_limit(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test that caching works correctly with row limits."""
        cache_path = os.path.join(temp_data_dir, "cache_limited.parquet")

        setup_providers(csv_path=sample_csv_file, cache_path=cache_path, numrows=3)

        # First call - creates cache with 3 rows
        gatherer1 = DaskDataGatherer()
        data1 = gatherer1.gather_data()
        assert len(data1.compute()) == 3

        # Second call - reads cache with 3 rows
        gatherer2 = DaskDataGatherer()
        data2 = gatherer2.gather_data()
        assert len(data2.compute()) == 3


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererSplitting:
    """Tests for large dataset splitting functionality."""

    def test_split_large_data(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test splitting large dataset into partitioned Parquet."""
        split_path = os.path.join(temp_data_dir, "split.parquet")

        setup_providers(csv_path=sample_csv_file, lines_per_file=2)

        gatherer = DaskDataGatherer()

        # Gather data first
        gatherer.gather_data()

        # Split data
        result = gatherer.split_large_data()

        # Verify return value for chaining
        assert result is gatherer

        # Verify partitioned Parquet created
        assert os.path.isdir(split_path)

        # Verify can read partitioned data
        split_df = dd.read_parquet(split_path)
        assert len(split_df.compute()) == 5  # Original row count preserved

    def test_split_large_data_skip_if_exists(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test that split_large_data skips if partitioned files exist."""
        split_path = os.path.join(temp_data_dir, "split.parquet")

        setup_providers(csv_path=sample_csv_file, lines_per_file=2)

        # Create gatherer and split once
        gatherer1 = DaskDataGatherer()
        gatherer1.gather_data()
        gatherer1.split_large_data()

        # Get modification time of first split
        first_mtime = os.path.getmtime(split_path)

        # Create new gatherer and split again (should skip)
        gatherer2 = DaskDataGatherer()
        gatherer2.gather_data()
        gatherer2.split_large_data()

        # Verify files not recreated (same modification time)
        second_mtime = os.path.getmtime(split_path)
        assert first_mtime == second_mtime

    def test_split_large_data_partition_count(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test that split_large_data creates correct number of partitions."""
        split_path = os.path.join(temp_data_dir, "split_partitioned.parquet")

        # 5 rows with 2 lines per file = 3 partitions
        setup_providers(csv_path=sample_csv_file, lines_per_file=2)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()
        gatherer.split_large_data()

        # Read partitioned data and check partition count
        split_df = dd.read_parquet(split_path)
        # Expected: ceil(5/2) = 3 partitions
        assert split_df.npartitions == 3


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererMemory:
    """Tests for memory management functionality."""

    def test_persist_data(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test persist_data method."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()

        # Persist data
        result = gatherer.persist_data()

        # Verify return value for chaining
        assert result is gatherer

        # Verify data is still accessible
        assert gatherer.data is not None
        assert len(gatherer.data.compute()) == 5

    def test_persist_data_without_gather_raises_error(self, setup_providers, temp_data_dir, dask_client):
        """Test persist_data raises error if data not gathered."""
        setup_providers()

        gatherer = DaskDataGatherer()

        # Should raise ValueError
        with pytest.raises(ValueError, match="No data gathered yet"):
            gatherer.persist_data()

    def test_get_memory_usage(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test get_memory_usage method."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()

        # Get memory usage
        memory_info = gatherer.get_memory_usage()

        # Verify structure
        assert isinstance(memory_info, dict)
        # Should have 'total' key
        assert 'total' in memory_info
        assert 'used_gb' in memory_info['total']
        assert 'limit_gb' in memory_info['total']
        assert 'percent' in memory_info['total']

    def test_log_memory_usage(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test log_memory_usage method doesn't raise errors."""
        setup_providers(csv_path=sample_csv_file)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()

        # Should not raise any errors
        gatherer.log_memory_usage()


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererWithRealData:
    """Integration tests using real sample datasets."""

    @pytest.mark.parametrize("dataset_size", ["1k", "10k"])
    def test_gather_from_sample_datasets(self, setup_providers, dataset_size, temp_data_dir, dask_client):
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

        gatherer = DaskDataGatherer()
        data = gatherer.gather_data()

        # Verify expected row count
        expected_counts = {"1k": 1000, "10k": 10000}
        assert len(data.compute()) == expected_counts[dataset_size]

        # Verify schema
        assert set(data.columns.tolist()) == set(get_bsm_raw_column_names())


@pytest.mark.dask
@pytest.mark.integration
class TestDaskDataGathererEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_gather_empty_csv(self, setup_providers, temp_data_dir, dask_client):
        """Test gathering from empty CSV file."""
        csv_path = os.path.join(temp_data_dir, "empty.csv")

        # Create CSV with just header (use minimal columns for test)
        header_cols = ["metadata_generatedAt", "coreData_id", "coreData_speed"]
        pd.DataFrame(columns=header_cols).to_csv(csv_path, index=False)

        setup_providers(csv_path=csv_path)

        gatherer = DaskDataGatherer()
        data = gatherer.gather_data()

        # Verify empty data
        assert len(data.compute()) == 0
        assert set(data.columns.tolist()) == set(header_cols)

    def test_gather_with_zero_numrows(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test gathering with numrows=0 returns all data."""
        setup_providers(csv_path=sample_csv_file, numrows=0)

        gatherer = DaskDataGatherer()
        data = gatherer.gather_data()

        # numrows=0 should be treated as "no limit"
        assert len(data.compute()) == 5

    def test_split_with_single_partition(self, setup_providers, sample_csv_file, temp_data_dir, dask_client):
        """Test splitting with lines_per_file larger than dataset."""
        split_path = os.path.join(temp_data_dir, "split_single.parquet")

        # 5 rows with 1000 lines per file = 1 partition
        setup_providers(csv_path=sample_csv_file, lines_per_file=1000)

        gatherer = DaskDataGatherer()
        gatherer.gather_data()
        gatherer.split_large_data()

        # Verify single partition created
        split_df = dd.read_parquet(split_path)
        assert split_df.npartitions == 1
        assert len(split_df.compute()) == 5
