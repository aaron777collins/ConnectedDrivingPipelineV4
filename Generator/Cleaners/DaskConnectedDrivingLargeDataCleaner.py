"""
DaskConnectedDrivingLargeDataCleaner - Dask implementation of ConnectedDrivingLargeDataCleaner.

This class provides Dask-based large dataset cleaning for Connected Driving BSM datasets.
Instead of file splitting and combining (pandas approach), this uses Dask partitioning for
distributed processing of large datasets.

Key differences from pandas version:
- Uses Parquet partitioning instead of CSV file splitting
- No explicit file combining - Dask handles partitioned data transparently
- Uses DaskParquetCache instead of CSVCache
- DataFrame operations are distributed across workers
- clean_data() processes entire dataset in parallel instead of sequential file iteration
- combine_data() is replaced by reading the partitioned Parquet directory
- getNRows() uses .head() instead of pd.read_csv(nrows=n)

Key differences from SparkConnectedDrivingLargeDataCleaner:
- Uses DaskSessionManager instead of SparkSessionManager
- Uses dd.read_parquet() instead of spark.read.parquet()
- Uses .compute() to trigger computation (no automatic execution like Spark)
- Uses .head(n, npartitions=-1) instead of .limit(n)
- Uses len(df) instead of df.count() for row counts
"""

import os
import os.path as path
import dask.dataframe as dd
from dask.dataframe import DataFrame

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.DaskDataGatherer import DaskDataGatherer
from Helpers.DaskSessionManager import DaskSessionManager

from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


class DaskConnectedDrivingLargeDataCleaner:
    """
    Dask-based cleaner for large Connected Driving BSM datasets.

    This cleaner processes large datasets using Dask's distributed computing capabilities:
    1. Reads data using DaskDataGatherer (from Parquet or CSV)
    2. Cleans data using configurable cleaner class (e.g., DaskCleanWithTimestamps)
    3. Optionally filters cleaned data (e.g., spatial/temporal filtering)
    4. Saves cleaned data as partitioned Parquet for efficient access
    5. Provides utility methods to read samples or full dataset

    Architecture:
    - splitfilespath: Replaced by Parquet partitions (not individual files)
    - cleanedfilespath: Directory for cleaned Parquet partitions
    - combinedcleandatapath: Single Parquet directory (replaces combined CSV file)

    Usage:
        cleaner = DaskConnectedDrivingLargeDataCleaner()
        cleaner.clean_data()  # Process entire dataset in parallel
        df_sample = cleaner.getNRows(10000)  # Get 10k rows
        df_all = cleaner.getAllRows()  # Get all cleaned data
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskConnectedDrivingLargeDataCleaner.

        Args:
            generatorPathProvider: Provides generator file system paths
            initialGathererPathProvider: Provides gatherer file system paths
            generatorContextProvider: Provides configuration context
        """
        self._generatorPathProvider = generatorPathProvider()
        self._initialgathererpathprovider = initialGathererPathProvider()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("DaskConnectedDrivingLargeDataCleaner")

        # Get Dask client (initialize if needed)
        self.client = DaskSessionManager.get_instance().get_client()

        # Path configuration - NOTE: For Dask, these are Parquet directories, not individual files
        # splitfilespath: Input data directory (partitioned Parquet from DaskDataGatherer)
        self.splitfilespath = self._initialgathererpathprovider.getPathWithModelName("DataGatherer.splitfilespath")

        # cleanedfilespath: Output directory for cleaned partitions (intermediate)
        self.cleanedfilespath = self._generatorPathProvider.getPathWithModelName("ConnectedDrivingLargeDataCleaner.cleanedfilespath")

        # combinedcleandatapath: Final combined cleaned data (Parquet directory)
        self.combinedcleandatapath = self._generatorPathProvider.getPathWithModelName("ConnectedDrivingLargeDataCleaner.combinedcleandatapath")

        # Create directories if they don't exist
        os.makedirs(path.dirname(self.splitfilespath), exist_ok=True)
        os.makedirs(path.dirname(self.cleanedfilespath), exist_ok=True)
        os.makedirs(path.dirname(self.combinedcleandatapath), exist_ok=True)

        # Get cleaning configuration
        self.x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        self.y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")
        self.max_dist = self._generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.max_dist")

        # Cleaner class and functions (e.g., DaskCleanWithTimestamps, DaskCleanerWithFilterWithinRangeXY)
        self.cleanerClass = self._generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.cleanerClass")
        self.cleanFunc = self._generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.cleanFunc")
        self.filterFunc = self._generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.filterFunc")

        # Column names for position
        self.pos_lat_col = "y_pos"
        self.pos_long_col = "x_pos"
        self.x_col = "x_pos"
        self.y_col = "y_pos"

    def clean_data(self):
        """
        Execute data cleaning pipeline on entire dataset using Dask's distributed processing.

        Instead of iterating through individual CSV files (pandas approach), this method:
        1. Checks if cleaned data already exists (skip if present)
        2. Reads split data from DaskDataGatherer output (partitioned Parquet)
        3. Applies cleaner function to entire DataFrame (distributed operation)
        4. Optionally applies filter function
        5. Writes cleaned data as partitioned Parquet

        Returns:
            self: For method chaining
        """
        # Check if cleaned data already exists
        if path.exists(self.combinedcleandatapath) and self._is_valid_parquet_directory(self.combinedcleandatapath):
            self.logger.log("Found cleaned data! Skipping regeneration.")
            return self

        self.logger.log("Starting large dataset cleaning with Dask...")

        # Read split data (from DataGatherer output)
        # Note: DaskDataGatherer outputs partitioned Parquet, not individual CSV files
        if path.exists(self.splitfilespath) and self._is_valid_parquet_directory(self.splitfilespath):
            self.logger.log(f"Reading split data from {self.splitfilespath}")
            df = dd.read_parquet(self.splitfilespath)
        else:
            # If split files don't exist, use DataGatherer to create them
            self.logger.log("Split files not found. Using DaskDataGatherer to gather and split data...")
            gatherer = DaskDataGatherer()
            gatherer.gather_data()
            gatherer.split_large_data()
            df = dd.read_parquet(self.splitfilespath)

        # Get row count (expensive operation in Dask, so we do it once)
        row_count = len(df)
        self.logger.log(f"Loaded {row_count} rows for cleaning")

        # Apply cleaner function to entire DataFrame
        # Note: cleanerClass is a Dask cleaner (e.g., DaskCleanWithTimestamps)
        self.logger.log(f"Applying cleaner: {self.cleanerClass.__name__}")
        cleaner_instance = self.cleanerClass(data=df)
        cleaned_df = self.cleanFunc(cleaner_instance).get_cleaned_data()

        # Apply optional filter function
        if self.filterFunc is not None:
            self.logger.log("Applying filter function...")
            cleaned_df = self.filterFunc(self, cleaned_df)

        # Check if cleaned DataFrame is empty
        cleaned_row_count = len(cleaned_df)
        if cleaned_row_count == 0:
            self.logger.log("Warning: Cleaned DataFrame is empty. No data to write.")
            return self

        # Write cleaned data as partitioned Parquet
        self.logger.log(f"Writing cleaned data to {self.combinedcleandatapath}")
        cleaned_df.to_parquet(
            self.combinedcleandatapath,
            engine='pyarrow',
            compression='snappy',
            write_index=False,
            overwrite=True
        )

        self.logger.log(f"Successfully cleaned {cleaned_row_count} rows")
        return self

    def combine_data(self):
        """
        Combine cleaned data (no-op for Dask - data is already combined in Parquet directory).

        In the pandas version, this method combines multiple CSV files into one large file.
        For Dask, the partitioned Parquet directory already functions as a single logical dataset,
        so this method simply validates that the cleaned data exists.

        Returns:
            self: For method chaining
        """
        # Check if combined data already exists
        if path.exists(self.combinedcleandatapath) and self._is_valid_parquet_directory(self.combinedcleandatapath):
            self.logger.log("Found combined data file! No combining needed (Parquet is already partitioned).")
            return self

        # If clean_data() hasn't been run, log a warning
        self.logger.log("Warning: Combined data not found. Call clean_data() first.")
        return self

    def getNRows(self, n):
        """
        Get N rows from the cleaned dataset.

        Instead of pd.read_csv(nrows=n), this uses .head(n, npartitions=-1) for efficient sampling.
        Note: Unlike pandas, this may not be deterministic without explicit ordering.

        Args:
            n (int): Number of rows to retrieve

        Returns:
            DataFrame: Dask DataFrame with N rows (lazy evaluation - use .compute() to materialize)
        """
        self.logger.log(f"Reading {n} rows from cleaned data...")

        # Check if combined data exists
        if not path.exists(self.combinedcleandatapath) or not self._is_valid_parquet_directory(self.combinedcleandatapath):
            raise FileNotFoundError(
                f"Combined cleaned data not found at {self.combinedcleandatapath}. "
                "Call clean_data() first."
            )

        # Read and limit to N rows
        df = dd.read_parquet(self.combinedcleandatapath)
        df_limited = df.head(n, npartitions=-1)

        self.logger.log(f"Successfully retrieved {n} rows (lazy - use .compute() to materialize)")
        return df_limited

    def getNumOfRows(self):
        """
        Get total number of rows in the cleaned dataset.

        Returns:
            int: Total row count
        """
        if not path.exists(self.combinedcleandatapath) or not self._is_valid_parquet_directory(self.combinedcleandatapath):
            raise FileNotFoundError(
                f"Combined cleaned data not found at {self.combinedcleandatapath}. "
                "Call clean_data() first."
            )

        df = dd.read_parquet(self.combinedcleandatapath)
        count = len(df)
        self.logger.log(f"Total rows in cleaned dataset: {count}")
        return count

    def getAllRows(self):
        """
        Get all rows from the cleaned dataset.

        Returns:
            DataFrame: Dask DataFrame with all cleaned data (lazy evaluation - use .compute() to materialize)
        """
        self.logger.log("Reading all rows from cleaned data...")

        if not path.exists(self.combinedcleandatapath) or not self._is_valid_parquet_directory(self.combinedcleandatapath):
            raise FileNotFoundError(
                f"Combined cleaned data not found at {self.combinedcleandatapath}. "
                "Call clean_data() first."
            )

        df = dd.read_parquet(self.combinedcleandatapath)
        row_count = len(df)
        self.logger.log(f"Successfully loaded {row_count} rows (lazy - use .compute() to materialize)")
        return df

    def _is_valid_parquet_directory(self, path_to_check):
        """
        Check if a path is a valid Parquet directory.

        A valid Parquet directory should contain either:
        - .parquet files, or
        - _SUCCESS file (indicating successful write)

        Args:
            path_to_check (str): Path to check

        Returns:
            bool: True if valid Parquet directory, False otherwise
        """
        if not path.exists(path_to_check):
            return False

        if not os.path.isdir(path_to_check):
            return False

        # Check for Parquet files or _SUCCESS marker
        files = os.listdir(path_to_check)
        has_parquet = any(f.endswith('.parquet') for f in files)
        has_success = '_SUCCESS' in files

        return has_parquet or has_success
