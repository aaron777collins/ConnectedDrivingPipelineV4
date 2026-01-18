"""
SparkConnectedDrivingCleaner - PySpark implementation of ConnectedDrivingCleaner.

This class provides PySpark-based data cleaning for Connected Driving BSM datasets,
replacing pandas operations with distributed Spark DataFrame transformations.

Key differences from pandas version:
- Uses ParquetCache instead of CSVCache for distributed caching
- Uses PySpark UDFs for coordinate conversions
- DataFrame operations are immutable (no inplace=True)
- Integrates with SparkDataGatherer instead of pandas DataGatherer
"""

import os.path as path
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame

from Decorators.ParquetCache import ParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.SparkDataGatherer import SparkDataGatherer

from Generator.Cleaners.IConnectedDrivingCleaner import IConnectedDrivingCleaner
from Helpers.SparkUDFs.GeospatialUDFs import point_to_x_udf, point_to_y_udf, geodesic_distance_udf
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class SparkConnectedDrivingCleaner(IConnectedDrivingCleaner):
    """
    PySpark-based cleaner for Connected Driving BSM data.

    This cleaner performs the following operations:
    1. Select specified columns from raw BSM data
    2. Drop rows with null values
    3. Parse WKT POINT strings to extract x_pos and y_pos
    4. Optionally convert lat/long to XY coordinates (distance from origin)
    5. Cache cleaned data as Parquet files

    Usage:
        cleaner = SparkConnectedDrivingCleaner(data=raw_df)
        cleaner.clean_data()
        cleaned_df = cleaner.get_cleaned_data()
    """

    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider, contextProvider: IGeneratorContextProvider,
                 data=None, filename=None):
        """
        Initialize SparkConnectedDrivingCleaner.

        Args:
            pathProvider: Provides file system paths
            contextProvider: Provides configuration context
            data (DataFrame, optional): PySpark DataFrame to clean. If None, will auto-gather.
            filename (str, optional): Cache filename. Defaults to "clean{numrows}.parquet"
        """
        self._generatorPathProvider = pathProvider()
        self._generatorContextProvider = contextProvider()
        self.logger = Logger("SparkConnectedDrivingCleaner")

        # Get cleaning configuration from context provider
        self.clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        self.filename = filename

        if self.filename is None:
            # Default filename to clean{numrows}.parquet (changed from .csv)
            numrows = self._generatorContextProvider.get("DataGatherer.numrows")
            self.filename = self._generatorContextProvider.get(
                "ConnectedDrivingCleaner.filename",
                f"clean{numrows}.parquet"
            )

        self.isXYCoords = self._generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords")
        self.columns = self._generatorContextProvider.get("ConnectedDrivingCleaner.columns")

        self.data = data
        self.x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        self.y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

        self.shouldGatherAutomatically = self._generatorContextProvider.get(
            "ConnectedDrivingCleaner.shouldGatherAutomatically"
        )

        # Validate data type and auto-gather if needed
        if not isinstance(self.data, DataFrame) and self.shouldGatherAutomatically:
            self.logger.log("No data specified. Using the gatherer to get the data (gather_data func).")
            self.data = SparkDataGatherer().gather_data()
        elif not isinstance(self.data, DataFrame):
            raise ValueError(
                "No data specified. You may want to specify data or set "
                "ConnectedDrivingCleaner.shouldGatherAutomatically to True."
            )

        self.cleaned_data = None

    def clean_data(self):
        """
        Execute data cleaning pipeline and cache results.

        This method orchestrates the cleaning process:
        1. Calls _clean_data() which performs the actual transformations
        2. Caches the result as Parquet file
        3. Returns self for method chaining

        Returns:
            self: For method chaining
        """
        self.cleaned_data = self._clean_data(cache_variables=[
            self.__class__.__name__, self.isXYCoords,
            self.clean_params, self.filename, self.x_pos, self.y_pos
        ])
        return self

    @ParquetCache
    def _clean_data(self, cache_variables=["REPLACE_ME"]) -> DataFrame:
        """
        Perform the actual data cleaning operations (cached as Parquet).

        Operations performed:
        1. Select specified columns
        2. Drop rows with any null values
        3. Parse coreData_position (WKT POINT) to extract x_pos, y_pos
        4. Drop the original coreData_position column
        5. Optionally convert to XY coordinates (if isXYCoords=True)

        Args:
            cache_variables (list): Variables to include in cache key (for ParquetCache)

        Returns:
            DataFrame: Cleaned PySpark DataFrame

        Note:
            This method is decorated with @ParquetCache, so results are cached
            to avoid recomputation. Cache key includes class name, parameters,
            and configuration to ensure proper cache invalidation.
        """
        # Step 1: Select specified columns
        # PySpark equivalent of: df = df[self.columns]
        self.cleaned_data = self.data.select(*self.columns)

        # Step 2: Drop rows with null values
        # PySpark equivalent of: df = df.dropna()
        self.cleaned_data = self.cleaned_data.na.drop()

        # Step 3: Parse coreData_position (WKT POINT) to x_pos and y_pos
        # Pandas equivalent was:
        #   df["x_pos"] = df["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[0])
        #   df["y_pos"] = df["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[1])
        # PySpark uses UDFs instead of .map()
        self.cleaned_data = self.cleaned_data.withColumn(
            "x_pos",
            point_to_x_udf(col("coreData_position"))
        )
        self.cleaned_data = self.cleaned_data.withColumn(
            "y_pos",
            point_to_y_udf(col("coreData_position"))
        )

        # Step 4: Drop the original coreData_position column
        # Pandas equivalent was: df.drop(columns=["coreData_position"], inplace=True)
        # PySpark DataFrames are immutable, so we reassign
        self.cleaned_data = self.cleaned_data.drop("coreData_position")

        # Step 5: Optionally convert to XY coordinates
        if self.isXYCoords:
            self.convert_to_XY_Coordinates()

        return self.cleaned_data

    def convert_to_XY_Coordinates(self):
        """
        Convert lat/long coordinates to XY distance from origin point.

        This method replicates the EXACT logic from the pandas version, even though
        the parameter semantics are confusing. The pandas code passes parameters in
        a way that doesn't match the function signature documentation, but we must
        replicate this for equivalence.

        Pandas equivalent:
            df["x_pos"] = df["x_pos"].map(lambda x: MathHelper.dist_between_two_points(x, self.y_pos, self.x_pos, self.y_pos))
            df["y_pos"] = df["y_pos"].map(lambda y: MathHelper.dist_between_two_points(self.x_pos, y, self.x_pos, self.y_pos))

        Where:
            - x is the current row's x_pos (longitude from POINT parsing)
            - y is the current row's y_pos (latitude from POINT parsing)
            - self.x_pos is the origin latitude (confusingly named)
            - self.y_pos is the origin longitude (confusingly named)

        Returns:
            self: For method chaining
        """
        # Replicate EXACT pandas logic for x_pos conversion
        # dist_between_two_points(lat1, lon1, lat2, lon2)
        # Pandas: MathHelper.dist_between_two_points(x, self.y_pos, self.x_pos, self.y_pos)
        # where x = current x_pos (longitude)
        self.cleaned_data = self.cleaned_data.withColumn(
            "x_pos",
            geodesic_distance_udf(
                col("x_pos"),           # lat1 (actually longitude, but pandas passes it here)
                lit(self.y_pos),        # lon1 (origin longitude)
                lit(self.x_pos),        # lat2 (origin latitude)
                lit(self.y_pos)         # lon2 (origin longitude)
            )
        )

        # Replicate EXACT pandas logic for y_pos conversion
        # Pandas: MathHelper.dist_between_two_points(self.x_pos, y, self.x_pos, self.y_pos)
        # where y = current y_pos (latitude)
        self.cleaned_data = self.cleaned_data.withColumn(
            "y_pos",
            geodesic_distance_udf(
                lit(self.x_pos),        # lat1 (origin latitude)
                col("y_pos"),           # lon1 (actually latitude, but pandas passes it here)
                lit(self.x_pos),        # lat2 (origin latitude)
                lit(self.y_pos)         # lon2 (origin longitude)
            )
        )

        return self

    def clean_data_with_timestamps(self):
        """
        Clean data with timestamp features (not implemented in base class).

        This method is required by IConnectedDrivingCleaner interface but is
        implemented in subclasses like SparkCleanWithTimestamps.

        Raises:
            NotImplementedError: This base class doesn't implement timestamp cleaning
        """
        raise NotImplementedError(
            "clean_data_with_timestamps is not implemented in SparkConnectedDrivingCleaner. "
            "Use SparkCleanWithTimestamps for timestamp feature extraction."
        )

    def get_cleaned_data(self):
        """
        Get the cleaned DataFrame.

        Returns:
            DataFrame: Cleaned PySpark DataFrame

        Raises:
            ValueError: If clean_data() hasn't been called yet
        """
        if self.cleaned_data is None:
            raise ValueError(
                "No cleaned data available. Call clean_data() first."
            )
        return self.cleaned_data
