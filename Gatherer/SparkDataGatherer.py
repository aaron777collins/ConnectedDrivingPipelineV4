"""
SparkDataGatherer - PySpark implementation of the DataGatherer class.

This class provides the same functionality as DataGatherer but uses PySpark
DataFrames instead of pandas DataFrames for distributed data processing.

Key Features:
- Reads CSV files using spark.read.csv with schema validation
- Caches results as Parquet files (via ParquetCache decorator)
- Supports row limiting via DataFrame.limit()
- Compatible with existing dependency injection framework
- Maintains same interface as DataGatherer for easy migration

Usage:
    gatherer = SparkDataGatherer()
    data = gatherer.gather_data()  # Returns pyspark.sql.DataFrame
"""

from pyspark.sql import DataFrame as SparkDataFrame
import os.path as path
import os

from Decorators.ParquetCache import ParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.IDataGatherer import IDataGatherer
from Helpers.SparkSessionManager import SparkSessionManager
from Schemas.BSMRawSchema import get_bsm_raw_schema
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


class SparkDataGatherer(IDataGatherer):
    """
    PySpark-based data gatherer for reading and caching BSM datasets.

    Replaces pandas-based DataGatherer with distributed Spark processing.
    Uses Parquet format for caching instead of CSV for better performance.
    """

    @StandardDependencyInjection
    def __init__(self, pathprovider: IInitialGathererPathProvider, contextprovider: IGeneratorContextProvider):
        """
        Initialize SparkDataGatherer with dependency injection.

        Args:
            pathprovider: Provider for file paths (injected)
            contextprovider: Provider for configuration context (injected)
        """
        self._initialGathererPathProvider = pathprovider()
        self._generatorContextProvider = contextprovider()
        self.logger = Logger("SparkDataGatherer")

        # Get or create Spark session
        self.spark = SparkSessionManager.get_session()

        # Initialize data and configuration
        self.data = None
        self.numrows = self._generatorContextProvider.get("DataGatherer.numrows")
        self.filepath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.filepath")
        self.subsectionpath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.subsectionpath")
        self.splitfilespath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.splitfilespath")

        # Convert CSV cache path to Parquet format
        # Replace .csv extension with .parquet for cached files
        if self.subsectionpath and self.subsectionpath.endswith('.csv'):
            self.subsectionpath = self.subsectionpath[:-4] + '.parquet'

    def gather_data(self) -> SparkDataFrame:
        """
        Gather data from source file (with caching).

        Returns:
            pyspark.sql.DataFrame: The gathered dataset
        """
        self.data = self._gather_data(full_file_cache_path=self.subsectionpath)
        return self.data

    @ParquetCache
    def _gather_data(self, full_file_cache_path="REPLACE_ME") -> SparkDataFrame:
        """
        Internal method to read CSV data using Spark with schema validation.
        Results are cached as Parquet files via ParquetCache decorator.

        Args:
            full_file_cache_path: Path for cache file (used by ParquetCache)

        Returns:
            pyspark.sql.DataFrame: The loaded dataset
        """
        self.logger.log("Didn't find file. Reading from full dataset with Spark.")

        # Get BSM raw schema
        schema = get_bsm_raw_schema()

        # Read CSV with schema
        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(self.filepath)

        # Apply row limit if specified
        if self.numrows is not None and self.numrows > 0:
            self.logger.log(f"Limiting to {self.numrows} rows")
            df = df.limit(self.numrows)

        return df

    def split_large_data(self) -> 'SparkDataGatherer':
        """
        Split large dataset into partitioned Parquet files for distributed processing.

        Unlike pandas version which splits into multiple CSV files, this version
        uses Spark's built-in partitioning to write data as partitioned Parquet.

        Returns:
            SparkDataGatherer: Self for method chaining
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

        # Calculate number of partitions based on lines_per_file
        # Get total row count
        if self.data is None:
            self.gather_data()

        total_rows = self.data.count()
        num_partitions = max(1, (total_rows + lines_per_file - 1) // lines_per_file)

        self.logger.log(f"Writing {total_rows} rows across {num_partitions} partitions")

        # Repartition and write to Parquet
        self.data.repartition(num_partitions) \
            .write \
            .mode("overwrite") \
            .parquet(split_parquet_path)

        self.logger.log("Partitioned Parquet files created successfully")

        return self

    def get_gathered_data(self) -> SparkDataFrame:
        """
        Get the gathered data.

        Returns:
            pyspark.sql.DataFrame: The gathered dataset
        """
        return self.data
