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
- Can delegate to S3DataGatherer for S3-based data sources (WYDOT integration)

Usage:
    # CSV mode (traditional)
    gatherer = DaskDataGatherer()
    data = gatherer.gather_data()  # Returns dask.dataframe.DataFrame

    # S3 mode (when S3DataGatherer configuration is present)
    # Set S3DataGatherer.source and S3DataGatherer.message_type in config
    gatherer = DaskDataGatherer()
    data = gatherer.gather_data()  # Automatically delegates to S3DataGatherer

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

        Automatically detects if S3 configuration is present (S3DataGatherer.source)
        and delegates to S3DataGatherer when appropriate. Otherwise, uses traditional
        CSV file reading.

        Args:
            pathprovider: Provider for file paths (injected)
            contextprovider: Provider for configuration context (injected)
        """
        self._initialGathererPathProvider = pathprovider()
        self._generatorContextProvider = contextprovider()
        self.logger = Logger("DaskDataGatherer")

        # Check if S3 data source configuration is present
        self._use_s3_source = self._check_s3_config()

        if self._use_s3_source:
            self.logger.log("Detected S3 configuration - delegating to S3DataGatherer")
            # Import here to avoid circular dependencies
            from Gatherer.S3DataGatherer import S3DataGatherer
            self._s3_gatherer = S3DataGatherer(
                pathprovider=pathprovider,
                contextprovider=contextprovider
            )
            # Delegate client and data references
            self.client = self._s3_gatherer.client
            self.data = None
            return

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

    def _check_s3_config(self) -> bool:
        """
        Check if S3 data source configuration is present.

        Returns:
            bool: True if S3 configuration exists, False otherwise
        """
        try:
            # Check for S3DataGatherer.source - the key indicator
            source = self._generatorContextProvider.get("S3DataGatherer.source")
            return source is not None
        except:
            return False

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
            
            # Part2 fields (often contain JSON strings - must be str)
            'part2_vse_events': 'object',  # JSON string with vehicle events
            'part2_spve_tr_units': 'object',  # JSON string with trailer units
            'metadata_rmd_rxSource': 'object',  # May contain mixed types
        }

    def gather_data(self):
        """
        Gather data from source file (with caching).

        If S3 configuration is present, delegates to S3DataGatherer.
        Otherwise, reads from local CSV files.

        Returns:
            dask.dataframe.DataFrame: The gathered dataset (lazy)
        """
        if self._use_s3_source:
            self.data = self._s3_gatherer.gather_data()
            return self.data

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
        self.logger.log("Error tolerance enabled: on_bad_lines='skip' (malformed rows will be skipped)")

        # Read CSV with Dask
        # For large CSV files with complex quoted fields (JSON arrays, etc.),
        # check for pre-converted Parquet version first
        parquet_path = self.filepath.replace('.csv', '.parquet')
        if os.path.isdir(parquet_path):
            self.logger.log(f"Found pre-converted Parquet at {parquet_path}, using it...")
            df = dd.read_parquet(parquet_path)
            self.logger.log(f"Loaded {df.npartitions} partitions from Parquet")
            
            # Convert problematic object columns to string for pyarrow compatibility
            # This fixes "did not recognize Python value type when inferring an Arrow data type" errors
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('').astype(str)
        else:
            # For large CSV files with complex quoting, convert to Parquet first
            # This handles the quoting issues and provides better performance
            self.logger.log("Converting CSV to Parquet for reliable parsing...")
            self.logger.log(f"Output: {parquet_path}")
            
            import pandas as pd
            chunk_size = 500000  # 500k rows per chunk
            os.makedirs(parquet_path, exist_ok=True)
            
            try:
                total_rows = 0
                for i, chunk in enumerate(pd.read_csv(
                    self.filepath,
                    dtype=self.column_dtypes,
                    chunksize=chunk_size,
                    on_bad_lines='skip',
                    low_memory=False
                )):
                    # Write each chunk as a separate parquet file
                    chunk_path = os.path.join(parquet_path, f"part.{i:04d}.parquet")
                    chunk.to_parquet(chunk_path, index=False)
                    total_rows += len(chunk)
                    if (i + 1) % 10 == 0:
                        self.logger.log(f"Converted {total_rows:,} rows ({i + 1} chunks)...")
                
                self.logger.log(f"CSV to Parquet conversion complete: {total_rows:,} total rows")
                
                # Now read the parquet files with Dask
                df = dd.read_parquet(parquet_path)
                self.logger.log(f"Loaded {df.npartitions} partitions from converted Parquet")
                
            except Exception as e:
                self.logger.log(f"Chunked conversion encountered error: {e}")
                
                # Check if we have partial parquet files that are usable
                existing_files = [f for f in os.listdir(parquet_path) if f.endswith('.parquet')] if os.path.isdir(parquet_path) else []
                if existing_files:
                    self.logger.log(f"Found {len(existing_files)} existing parquet files, using partial conversion...")
                    df = dd.read_parquet(parquet_path)
                    row_count = len(df)
                    self.logger.log(f"Loaded {df.npartitions} partitions ({row_count:,} rows) from partial Parquet")
                else:
                    self.logger.log("No parquet files found, falling back to dask.read_csv...")
                    # Fallback to dask's native reader with robust error handling
                    df = dd.read_csv(
                        self.filepath,
                        dtype=self.column_dtypes,
                        blocksize=self.blocksize,
                        assume_missing=self.assume_missing,
                        on_bad_lines='skip'
                    )

        # Count loaded rows and estimate skipped lines
        # Note: This triggers a compute, but necessary for accurate skip counting
        try:
            loaded_rows = len(df)
            self.logger.log(f"Loaded {loaded_rows:,} rows from CSV")
            
            # Try to get source file line count for comparison
            import subprocess
            result = subprocess.run(
                ['wc', '-l', self.filepath],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                # wc -l output: "  12345 filename"
                source_lines = int(result.stdout.strip().split()[0])
                # Subtract 1 for header row
                expected_data_rows = source_lines - 1
                skipped_rows = expected_data_rows - loaded_rows
                if skipped_rows > 0:
                    self.logger.log(f"⚠️  SKIPPED {skipped_rows:,} malformed rows (source had {expected_data_rows:,} data rows)")
                else:
                    self.logger.log(f"✓ All {expected_data_rows:,} data rows loaded successfully (no rows skipped)")
        except Exception as e:
            self.logger.log(f"Could not count skipped rows: {e}")

        # Apply row limit if specified
        if self.numrows is not None and self.numrows > 0:
            self.logger.log(f"Limiting to {self.numrows} rows")
            # Sample uniformly across all partitions to get representative data
            # This avoids OOM from head(npartitions=-1) and temporal bias from npartitions=1
            total_rows = len(df)
            if total_rows > self.numrows:
                frac = min(1.0, (self.numrows * 1.2) / total_rows)  # 20% buffer for sample variance
                self.logger.log(f"Sampling {frac:.4f} fraction from {total_rows} rows across all partitions")
                sampled = df.sample(frac=frac, random_state=42).compute()
                # Trim to exact count
                if len(sampled) > self.numrows:
                    sampled = sampled.iloc[:self.numrows]
                self.logger.log(f"Got {len(sampled)} rows from sampling")
                df = dd.from_pandas(sampled, npartitions=max(1, len(sampled) // 500000))
            else:
                self.logger.log(f"Dataset already has {total_rows} rows (<= {self.numrows}), using all")

        self.logger.log(f"Loaded {df.npartitions} partitions")

        return df

    def split_large_data(self):
        """
        Split large dataset into partitioned Parquet files for distributed processing.

        Unlike pandas version which splits into multiple CSV files, this version
        uses Dask's built-in partitioning to write data as partitioned Parquet.

        Note: Not applicable when using S3 source (data is already partitioned).

        Returns:
            DaskDataGatherer: Self for method chaining
        """
        if self._use_s3_source:
            self.logger.log("split_large_data not applicable for S3 sources (already partitioned)")
            return self

        lines_per_file = self._generatorContextProvider.get("DataGatherer.lines_per_file")
        os.makedirs(path.dirname(self.splitfilespath) if not self.splitfilespath.endswith('/') else self.splitfilespath, exist_ok=True)

        # Convert splitfilespath to Parquet format
        # Handle both file paths (.csv) and directory paths (ending with /)
        if self.splitfilespath.endswith('.csv'):
            split_parquet_path = self.splitfilespath[:-4] + '.parquet'
        elif self.splitfilespath.endswith('/'):
            split_parquet_path = self.splitfilespath.rstrip('/') + '.parquet'
        elif self.splitfilespath.endswith('.parquet'):
            split_parquet_path = self.splitfilespath
        else:
            split_parquet_path = self.splitfilespath + '.parquet'

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
        if self._use_s3_source:
            return self._s3_gatherer.get_gathered_data()
        return self.data

    def compute_data(self):
        """
        Compute the Dask DataFrame to pandas DataFrame.

        WARNING: Only use this for small to medium datasets (<5M rows).
        For larger datasets, keep working with Dask operations.

        Returns:
            pandas.DataFrame: The computed dataset
        """
        if self._use_s3_source:
            return self._s3_gatherer.compute_data()

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
        if self._use_s3_source:
            self._s3_gatherer.persist_data()
            return self

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
        if self._use_s3_source:
            return self._s3_gatherer.get_memory_usage()
        return DaskSessionManager.get_memory_usage()

    def log_memory_usage(self):
        """
        Log current memory usage of the Dask cluster.
        """
        if self._use_s3_source:
            self._s3_gatherer.log_memory_usage()
            return

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
