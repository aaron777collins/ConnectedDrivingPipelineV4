"""
SparkCleanWithTimestamps - PySpark implementation of CleanWithTimestamps.

This class extends SparkConnectedDrivingCleaner to add timestamp feature extraction
using PySpark native datetime functions for optimal performance.

Key features:
- Parses timestamps using PySpark's to_timestamp() function
- Extracts temporal features (year, month, day, hour, minute, second, am/pm)
- Uses native Spark SQL functions (not UDFs) for Catalyst optimization
- Caches results as Parquet files for efficient storage
"""

from pyspark.sql.functions import (
    col, lit, to_timestamp,
    year, month, dayofmonth,
    hour, minute, second, when
)
from pyspark.sql import DataFrame

from Decorators.ParquetCache import ParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from Helpers.SparkUDFs.GeospatialUDFs import point_to_x_udf, point_to_y_udf
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class SparkCleanWithTimestamps(SparkConnectedDrivingCleaner):
    """
    PySpark-based cleaner for Connected Driving BSM data with timestamp features.

    This cleaner extends SparkConnectedDrivingCleaner by adding temporal feature
    extraction from the metadata_generatedAt timestamp column.

    Temporal features extracted:
    - month: Month (1-12)
    - day: Day of month (1-31)
    - year: Year (YYYY)
    - hour: Hour (0-23)
    - minute: Minute (0-59)
    - second: Second (0-59)
    - pm: AM/PM indicator (0 for AM, 1 for PM)

    Usage:
        cleaner = SparkCleanWithTimestamps(data=raw_df)
        cleaner.clean_data_with_timestamps()
        cleaned_df = cleaner.get_cleaned_data()

    Note: Uses PySpark native datetime functions instead of UDFs for better
    performance and optimization by Spark's Catalyst query optimizer.
    """

    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider,
                 contextProvider: IGeneratorContextProvider,
                 data=None, filename=None):
        """
        Initialize SparkCleanWithTimestamps.

        Args:
            pathProvider: Provides file system paths
            contextProvider: Provides configuration context
            data (DataFrame, optional): PySpark DataFrame to clean. If None, will auto-gather.
            filename (str, optional): Cache filename. Defaults to "clean{numrows}.parquet"
        """
        # Initialize base class to get basic dependencies and configuration
        # Don't pass pathProvider/contextProvider - they're injected by decorator
        super().__init__(data=data, filename=filename)

    def clean_data_with_timestamps(self):
        """
        Execute data cleaning pipeline with timestamp feature extraction and cache results.

        This method performs all standard cleaning operations plus temporal feature extraction:
        1. Select specified columns
        2. Drop null values
        3. Parse WKT POINT to x_pos/y_pos
        4. Parse metadata_generatedAt timestamp
        5. Extract temporal features (year, month, day, hour, minute, second, pm)
        6. Optionally convert to XY coordinates
        7. Cache as Parquet

        Returns:
            self: Returns self for method chaining

        Raises:
            ValueError: If data is not a valid PySpark DataFrame
        """
        self.cleaned_data = self._clean_data_with_timestamps(cache_variables=[
            self.__class__.__name__, self.isXYCoords,
            self.clean_params, self.filename, self.x_pos, self.y_pos
        ])
        return self

    @ParquetCache
    def _clean_data_with_timestamps(self, cache_variables=None) -> DataFrame:
        """
        Internal method that performs actual cleaning with timestamp extraction.

        Decorated with @ParquetCache for distributed caching.

        Args:
            cache_variables (list, optional): Variables used for cache key generation

        Returns:
            DataFrame: Cleaned PySpark DataFrame with temporal features

        Implementation notes:
        - Timestamp format: "%m/%d/%Y %I:%M:%S %p" (e.g., "07/31/2019 12:41:59 PM")
        - Uses PySpark native functions for optimal performance
        - All operations are immutable (creates new columns, doesn't modify existing)
        - Drops original position columns after extracting x_pos/y_pos
        """
        # Step 1: Select specified columns
        self.cleaned_data = self.data.select(*self.columns)

        # Step 2: Drop rows with null values
        self.cleaned_data = self.cleaned_data.na.drop()

        # Step 3: Parse WKT POINT to extract x_pos and y_pos
        # Format: "POINT (longitude latitude)" -> extract x (longitude) and y (latitude)
        self.cleaned_data = self.cleaned_data.withColumn(
            "x_pos",
            point_to_x_udf(col("coreData_position"))
        )
        self.cleaned_data = self.cleaned_data.withColumn(
            "y_pos",
            point_to_y_udf(col("coreData_position"))
        )

        # Step 4: Parse timestamp from string to timestamp type
        # Input format: "07/31/2019 12:41:59 PM" (MM/dd/yyyy hh:mm:ss a)
        # PySpark format string uses Java SimpleDateFormat patterns
        self.cleaned_data = self.cleaned_data.withColumn(
            "metadata_generatedAt",
            to_timestamp(col("metadata_generatedAt"), "MM/dd/yyyy hh:mm:ss a")
        )

        # Step 5: Extract temporal features using native PySpark functions
        # These operations are optimized by Catalyst and much faster than UDFs

        # Extract month (1-12)
        self.cleaned_data = self.cleaned_data.withColumn(
            "month",
            month(col("metadata_generatedAt"))
        )

        # Extract day of month (1-31)
        self.cleaned_data = self.cleaned_data.withColumn(
            "day",
            dayofmonth(col("metadata_generatedAt"))
        )

        # Extract year (YYYY)
        self.cleaned_data = self.cleaned_data.withColumn(
            "year",
            year(col("metadata_generatedAt"))
        )

        # Extract hour (0-23)
        self.cleaned_data = self.cleaned_data.withColumn(
            "hour",
            hour(col("metadata_generatedAt"))
        )

        # Extract minute (0-59)
        self.cleaned_data = self.cleaned_data.withColumn(
            "minute",
            minute(col("metadata_generatedAt"))
        )

        # Extract second (0-59)
        self.cleaned_data = self.cleaned_data.withColumn(
            "second",
            second(col("metadata_generatedAt"))
        )

        # Extract AM/PM indicator (0 for AM, 1 for PM)
        # Hour < 12 means AM (0), hour >= 12 means PM (1)
        self.cleaned_data = self.cleaned_data.withColumn(
            "pm",
            when(hour(col("metadata_generatedAt")) < 12, 0).otherwise(1)
        )

        # Step 6: Drop original position columns (no longer needed)
        self.cleaned_data = self.cleaned_data.drop(
            "coreData_position",
            "coreData_position_lat",
            "coreData_position_long"
        )

        # Step 7: Optionally convert to XY coordinates (distance from origin)
        if self.isXYCoords:
            self.convert_to_XY_Coordinates()

        return self.cleaned_data
