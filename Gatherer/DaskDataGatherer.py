"""
DaskDataGatherer - Dask implementation of the DataGatherer class.

This class provides the same functionality as DataGatherer but uses Dask
DataFrames instead of pandas DataFrames for distributed data processing.

Key Features:
- Reads CSV files using dd.read_csv with configurable block size
- Caches results as Parquet files (via DaskParquetCache decorator)
- Supports row limiting via DataFrame.head()
- Compatible with existing dependency injection framework
- Maintains same interface as DataGatherer for easy migration
- Memory-safe for 64GB systems with 15M+ row datasets

Usage:
    gatherer = DaskDataGatherer()
    data = gatherer.gather_data()  # Returns dask.dataframe.DataFrame

    # Convert to pandas for small datasets
    pandas_df = data.compute()

    # Or continue working with Dask
    filtered_data = data[data['speed'] > 0]
"""

import dask.dataframe as dd
import os.path as path
import os

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.IDataGatherer import IDataGatherer
from Helpers.DaskSessionManager import DaskSessionManager
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


class DaskDataGatherer(IDataGatherer):
    """
    Dask-based data gatherer for reading and caching BSM datasets.

    Replaces pandas-based DataGatherer with distributed Dask processing.
    Uses Parquet format for caching instead of CSV for better performance.
    Optimized for 64GB systems with 6 workers × 8GB memory.
    """

    @StandardDependencyInjection
    def __init__(self, pathprovider: IInitialGathererPathProvider, contextprovider: IGeneratorContextProvider):
        """
        Initialize DaskDataGatherer with dependency injection.

        Args:
            pathprovider: Provider for file paths (injected)
            contextprovider: Provider for configuration context (injected)
        """
        self._initialGathererPathProvider = pathprovider()
        self._generatorContextProvider = contextprovider()
        self.logger = Logger("DaskDataGatherer")

        # Get or create Dask client
        self.client = DaskSessionManager.get_client()
        self.logger.log(f"Dask dashboard: {self.client.dashboard_link}")

        # Initialize data and configuration
        self.data = None
        self.numrows = self._generatorContextProvider.get("DataGatherer.numrows")
        self.filepath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.filepath")
        self.subsectionpath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.subsectionpath")
        self.splitfilespath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.splitfilespath")

        # Dask-specific configuration
        try:
            self.blocksize = self._generatorContextProvider.get("DataGatherer.blocksize")
        except:
            # TASK 49: Reduced from 128MB to 64MB to lower peak memory usage
            # 64MB = ~225K rows/partition (vs 450K), reduces per-partition memory by ~50%
            # At 15M rows: 67 partitions @ 64MB vs 34 partitions @ 128MB
            # Lower partition size = better memory distribution, earlier spilling
            self.blocksize = '64MB'  # Default: ~225K rows per partition (TASK 49: memory optimization)

        try:
            self.assume_missing = self._generatorContextProvider.get("DataGatherer.assume_missing")
        except:
            self.assume_missing = True  # Faster parsing

        # Convert CSV cache path to Parquet format
        # Replace .csv extension with .parquet for cached files
        if self.subsectionpath and self.subsectionpath.endswith('.csv'):
            self.subsectionpath = self.subsectionpath[:-4] + '.parquet'

        # Get column dtypes from configuration (optional)
        try:
            self.column_dtypes = self._generatorContextProvider.get("DataGatherer.dtypes")
        except:
            self.column_dtypes = self._get_default_dtypes()

    def _get_default_dtypes(self):
        """
        Get default column dtypes for BSM raw data.

        This matches the PySpark schema but uses pandas/Dask dtype notation.

        Returns:
            dict: Column name to dtype mapping
        """
        return {
            # Metadata fields
            'metadata_generatedAt': 'object',  # Parse to datetime later
            'metadata_recordType': 'object',
            'metadata_serialId_streamId': 'object',
            'metadata_serialId_bundleSize': 'Int64',  # Nullable integer
            'metadata_serialId_bundleId': 'Int64',
            'metadata_serialId_recordId': 'Int64',
            'metadata_serialId_serialNumber': 'Int64',
            'metadata_receivedAt': 'object',  # Parse to datetime later

            # Core BSM data fields
            'coreData_id': 'object',  # Hex string
            'coreData_secMark': 'Int64',
            'coreData_position_lat': 'float64',
            'coreData_position_long': 'float64',
            'coreData_accuracy_semiMajor': 'float64',
            'coreData_accuracy_semiMinor': 'float64',
            'coreData_elevation': 'float64',
            'coreData_accelset_accelYaw': 'float64',
            'coreData_speed': 'float64',
            'coreData_heading': 'float64',
            'coreData_position': 'object',  # WKT POINT format
        }

    def gather_data(self):
        """
        Gather data from source file (with caching).

        Returns:
            dask.dataframe.DataFrame: The gathered dataset (lazy)
        """
        self.data = self._gather_data(full_file_cache_path=self.subsectionpath)
        return self.data

    @DaskParquetCache
    def _gather_data(self, full_file_cache_path="REPLACE_ME") -> dd.DataFrame:
        """
        Internal method to read CSV data using Dask with type validation.
        Results are cached as Parquet files via DaskParquetCache decorator.

        Args:
            full_file_cache_path: Path for cache file (used by DaskParquetCache)

        Returns:
            dask.dataframe.DataFrame: The loaded dataset (lazy)
        """
        self.logger.log("Didn't find file. Reading from full dataset with Dask.")
        self.logger.log(f"Blocksize: {self.blocksize}")

        # Read CSV with Dask
        df = dd.read_csv(
            self.filepath,
            dtype=self.column_dtypes,
            blocksize=self.blocksize,
            assume_missing=self.assume_missing
        )

        # Apply row limit if specified
        if self.numrows is not None and self.numrows > 0:
            self.logger.log(f"Limiting to {self.numrows} rows")
            # Use head() for row limiting in Dask
            # Note: This computes the first N rows, not lazy
            df = df.head(self.numrows, npartitions=-1)
            # Convert back to Dask DataFrame
            df = dd.from_pandas(df, npartitions=max(1, self.numrows // 500000))

        self.logger.log(f"Loaded {df.npartitions} partitions")

        return df

    def split_large_data(self):
        """
        Split large dataset into partitioned Parquet files for distributed processing.

        Unlike pandas version which splits into multiple CSV files, this version
        uses Dask's built-in partitioning to write data as partitioned Parquet.

        Returns:
            DaskDataGatherer: Self for method chaining
        """
        lines_per_file = self._generatorContextProvider.get("DataGatherer.lines_per_file")
        os.makedirs(path.dirname(self.splitfilespath), exist_ok=True)

        # Convert splitfilespath to Parquet format
        split_parquet_path = self.splitfilespath.replace('.csv', '.parquet')

        # Check if partitioned data already exists
        if path.isdir(split_parquet_path):
            self.logger.log("Found partitioned Parquet files! Skipping regeneration.")
            return self

        self.logger.log("Splitting large dataset into partitioned Parquet files...")

        # Gather data if not already loaded
        if self.data is None:
            self.gather_data()

        # Calculate number of partitions based on lines_per_file
        # Get total row count (requires computation, but only counts)
        total_rows = len(self.data)
        num_partitions = max(1, (total_rows + lines_per_file - 1) // lines_per_file)

        self.logger.log(f"Writing {total_rows} rows across {num_partitions} partitions")

        # Repartition and write to Parquet
        self.data.repartition(npartitions=num_partitions).to_parquet(
            split_parquet_path,
            engine='pyarrow',
            compression='snappy',
            write_index=False,
            overwrite=True
        )

        self.logger.log("Partitioned Parquet files created successfully")

        return self

    def get_gathered_data(self):
        """
        Get the gathered data.

        Returns:
            dask.dataframe.DataFrame: The gathered dataset (lazy)
        """
        return self.data

    def compute_data(self):
        """
        Compute the Dask DataFrame to pandas DataFrame.

        WARNING: Only use this for small to medium datasets (<5M rows).
        For larger datasets, keep working with Dask operations.

        Returns:
            pandas.DataFrame: The computed dataset
        """
        if self.data is None:
            raise ValueError("No data gathered yet. Call gather_data() first.")

        self.logger.log("Computing Dask DataFrame to pandas...")
        pandas_df = self.data.compute()
        self.logger.log(f"Computed {len(pandas_df)} rows to pandas DataFrame")

        return pandas_df

    def persist_data(self):
        """
        Persist the Dask DataFrame in distributed memory for faster reuse.

        Use this when you'll be accessing the same data multiple times.
        The data stays in Dask format (distributed across workers).

        Returns:
            DaskDataGatherer: Self for method chaining
        """
        if self.data is None:
            raise ValueError("No data gathered yet. Call gather_data() first.")

        self.logger.log("Persisting Dask DataFrame in distributed memory...")
        self.data = self.data.persist()
        self.logger.log("Data persisted successfully")

        return self

    def get_memory_usage(self):
        """
        Get current memory usage of the Dask cluster.

        Returns:
            dict: Memory usage information
        """
        return DaskSessionManager.get_memory_usage()

    def log_memory_usage(self):
        """
        Log current memory usage of the Dask cluster.
        """
        memory_info = self.get_memory_usage()

        if 'total' in memory_info:
            total = memory_info['total']
            self.logger.log(
                f"Cluster memory: {total['used_gb']:.2f}GB / {total['limit_gb']:.2f}GB "
                f"({total['percent']:.1f}%)"
            )

        for worker_addr, info in memory_info.items():
            if worker_addr != 'total':
                self.logger.log(
                    f"  {worker_addr}: {info['used_gb']:.2f}GB / {info['limit_gb']:.2f}GB "
                    f"({info['percent']:.1f}%)"
                )
