"""
S3DataGatherer - Adapter for DataSources module to implement IDataGatherer interface.

This adapter bridges the DataSources S3-based data fetching with the existing
pipeline's IDataGatherer interface, allowing seamless integration with existing
processing components.

Features:
- Implements IDataGatherer interface (gather_data, get_gathered_data)
- Wraps S3DataFetcher for S3-based data access
- Returns Dask DataFrames for compatibility with pipeline
- Supports caching via DaskParquetCache decorator
- Compatible with dependency injection framework

Usage:
    # Via dependency injection (recommended)
    gatherer = S3DataGatherer()
    data = gatherer.gather_data()  # Returns dask.dataframe.DataFrame

    # Or manual configuration
    gatherer = S3DataGatherer(pathprovider=..., contextprovider=...)
    data = gatherer.gather_data()
"""

import dask.dataframe as dd
import pandas as pd
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.IDataGatherer import IDataGatherer
from Helpers.DaskSessionManager import DaskSessionManager
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider

# Import DataSources components
from DataSources.config import DataSourceConfig
from DataSources.S3DataFetcher import S3DataFetcher
from DataSources.CacheManager import CacheManager


class S3DataGatherer(IDataGatherer):
    """
    Dask-based data gatherer for reading CV Pilot data from S3.

    Adapter that implements IDataGatherer interface using the DataSources
    module's S3DataFetcher for data retrieval. Returns Dask DataFrames
    for compatibility with existing pipeline components.

    Configuration via context provider:
    - S3DataGatherer.source: Data source (wydot, thea, nycdot)
    - S3DataGatherer.message_type: Message type (BSM, TIM, SPAT, EVENT)
    - S3DataGatherer.start_date: Start date (YYYY-MM-DD or date object)
    - S3DataGatherer.end_date: End date (YYYY-MM-DD or date object)
    - S3DataGatherer.days_back: Alternative to start_date (int)
    - S3DataGatherer.timezone: Timezone for dates (default: UTC)
    - S3DataGatherer.cache_dir: Cache directory (optional)
    - S3DataGatherer.subsectionpath: Cache file path (optional)
    """

    @StandardDependencyInjection
    def __init__(
        self,
        pathprovider: IInitialGathererPathProvider,
        contextprovider: IGeneratorContextProvider
    ):
        """
        Initialize S3DataGatherer with dependency injection.

        Args:
            pathprovider: Provider for file paths (injected)
            contextprovider: Provider for configuration context (injected)
        """
        self._initialGathererPathProvider = pathprovider()
        self._generatorContextProvider = contextprovider()
        self.logger = Logger("S3DataGatherer")

        # Get or create Dask client
        self.client = DaskSessionManager.get_client()
        self.logger.log(f"Dask dashboard: {self.client.dashboard_link}")

        # Initialize data
        self.data = None

        # Build DataSourceConfig from context provider
        self.config = self._build_config()

        # Get cache path for DaskParquetCache decorator
        try:
            self.subsectionpath = self._initialGathererPathProvider.getPathWithModelName(
                "S3DataGatherer.subsectionpath"
            )
        except:
            # Generate default cache path based on config
            cache_key = self.config.generate_cache_key()
            self.subsectionpath = str(
                self.config.cache.directory / cache_key / "data.parquet"
            )

        # Convert CSV extension to Parquet if needed
        if self.subsectionpath and self.subsectionpath.endswith('.csv'):
            self.subsectionpath = self.subsectionpath[:-4] + '.parquet'

        self.logger.log(
            f"Initialized S3DataGatherer for {self.config.source}/"
            f"{self.config.message_type} ({self.config.date_range.start_date} to "
            f"{self.config.date_range.end_date})"
        )

    def _build_config(self) -> DataSourceConfig:
        """
        Build DataSourceConfig from context and path providers.

        Returns:
            DataSourceConfig: Configuration for S3DataFetcher

        Raises:
            ValueError: If required configuration is missing
        """
        # Get required parameters
        source = self._get_config("S3DataGatherer.source")
        message_type = self._get_config("S3DataGatherer.message_type")

        # Get date range configuration
        start_date = self._get_config("S3DataGatherer.start_date", required=False)
        end_date = self._get_config("S3DataGatherer.end_date", required=False)
        days_back = self._get_config("S3DataGatherer.days_back", required=False)
        timezone = self._get_config("S3DataGatherer.timezone", default="UTC")

        # Parse dates if strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Build date range dict
        date_range = {"timezone": timezone}
        if start_date:
            date_range["start_date"] = start_date
        if end_date:
            date_range["end_date"] = end_date
        if days_back:
            date_range["days_back"] = days_back

        # Get optional cache directory
        cache_dir = self._get_config("S3DataGatherer.cache_dir", required=False)

        # Build config dict
        config_dict = {
            "source": source,
            "message_type": message_type,
            "date_range": date_range,
        }

        if cache_dir:
            config_dict["cache"] = {"directory": cache_dir}

        # Create and validate config
        return DataSourceConfig(**config_dict)

    def _get_config(self, key: str, required: bool = True, default=None):
        """
        Get configuration value from context provider.

        Args:
            key: Configuration key
            required: Whether the key is required
            default: Default value if not found

        Returns:
            Configuration value

        Raises:
            ValueError: If required key is missing
        """
        try:
            return self._generatorContextProvider.get(key)
        except:
            if required:
                raise ValueError(f"Required configuration missing: {key}")
            return default

    def gather_data(self):
        """
        Gather data from S3 (with caching).

        Returns:
            dask.dataframe.DataFrame: The gathered dataset (lazy)
        """
        self.data = self._gather_data(full_file_cache_path=self.subsectionpath)
        return self.data

    @DaskParquetCache
    def _gather_data(self, full_file_cache_path="REPLACE_ME") -> dd.DataFrame:
        """
        Internal method to fetch data from S3 and convert to Dask DataFrame.
        Results are cached as Parquet files via DaskParquetCache decorator.

        Args:
            full_file_cache_path: Path for cache file (used by DaskParquetCache)

        Returns:
            dask.dataframe.DataFrame: The loaded dataset (lazy)
        """
        self.logger.log("Cache miss. Fetching data from S3...")

        # Create S3DataFetcher and CacheManager
        fetcher = S3DataFetcher(self.config)
        cache_mgr = CacheManager(self.config)

        # Check if data is already in cache
        cache_key = self.config.generate_cache_key()
        cache_dir = self.config.cache.directory / cache_key

        if cache_mgr.is_cached(cache_key):
            self.logger.log(f"Found cached data at {cache_dir}")

            # Load cached Parquet files
            parquet_files = list(cache_dir.glob("*.parquet"))
            if parquet_files:
                self.logger.log(f"Loading {len(parquet_files)} cached Parquet files")
                df = dd.read_parquet(cache_dir / "*.parquet", engine='pyarrow')
                self.logger.log(f"Loaded {df.npartitions} partitions from cache")
                return df

        # Fetch data from S3
        self.logger.log(
            f"Fetching data from S3: {self.config.source}/{self.config.message_type}"
        )

        # List files in date range
        s3_objects = fetcher.list_files(
            self.config.date_range.start_date,
            self.config.date_range.end_date
        )

        if not s3_objects:
            self.logger.log("No data found for specified date range")
            # Return empty DataFrame with expected schema
            return dd.from_pandas(pd.DataFrame(), npartitions=1)

        self.logger.log(f"Found {len(s3_objects)} files to download")

        # Download files
        download_results = fetcher.download_files(s3_objects, cache_dir)

        # Count successful downloads
        successful = sum(1 for r in download_results if r.success)
        self.logger.log(f"Downloaded {successful}/{len(s3_objects)} files successfully")

        # Parse and cache as Parquet
        self.logger.log("Parsing JSON and caching as Parquet...")
        cache_mgr.cache_data(cache_key, download_results, self.config)

        # Load cached Parquet files
        parquet_files = list(cache_dir.glob("*.parquet"))
        if not parquet_files:
            self.logger.log("No Parquet files created (no valid data)")
            return dd.from_pandas(pd.DataFrame(), npartitions=1)

        self.logger.log(f"Loading {len(parquet_files)} Parquet files")
        df = dd.read_parquet(cache_dir / "*.parquet", engine='pyarrow')
        self.logger.log(f"Loaded {df.npartitions} partitions")

        return df

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
            S3DataGatherer: Self for method chaining
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
