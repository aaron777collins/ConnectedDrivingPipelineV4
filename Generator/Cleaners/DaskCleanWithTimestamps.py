"""
DaskCleanWithTimestamps - Dask implementation of CleanWithTimestamps.

This class extends DaskConnectedDrivingCleaner to add timestamp feature extraction
using Dask's distributed datetime operations for optimal performance.

Key features:
- Parses timestamps using Dask's dd.to_datetime() function
- Extracts temporal features (year, month, day, hour, minute, second, am/pm)
- Uses map_partitions for efficient feature extraction
- Caches results as Parquet files for efficient storage

Compatibility:
- Matches SparkCleanWithTimestamps interface and behavior
- Produces identical output to pandas CleanWithTimestamps
- Uses same timestamp format: "%m/%d/%Y %I:%M:%S %p"
"""

import dask.dataframe as dd
from dask.dataframe import DataFrame
import pandas as pd

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Helpers.DaskUDFs import point_to_x, point_to_y
from Helpers.DaskUDFs.MapPartitionsWrappers import extract_xy_coordinates
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class DaskCleanWithTimestamps(DaskConnectedDrivingCleaner):
    """
    Dask-based cleaner for Connected Driving BSM data with timestamp features.

    This cleaner extends DaskConnectedDrivingCleaner by adding temporal feature
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
        cleaner = DaskCleanWithTimestamps(data=raw_df)
        cleaner.clean_data_with_timestamps()
        cleaned_df = cleaner.get_cleaned_data()

    Note: Uses Dask's dd.to_datetime() and map_partitions for efficient
    distributed processing of temporal features.
    """

    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider,
                 contextProvider: IGeneratorContextProvider,
                 data=None, filename=None):
        """
        Initialize DaskCleanWithTimestamps.

        Args:
            pathProvider: Provides file system paths
            contextProvider: Provides configuration context
            data (DataFrame, optional): Dask DataFrame to clean. If None, will auto-gather.
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
            ValueError: If data is not a valid Dask DataFrame
        """
        self.cleaned_data = self._clean_data_with_timestamps(cache_variables=[
            self.__class__.__name__, self.isXYCoords,
            self.clean_params, self.filename, self.x_pos, self.y_pos
        ])
        return self

    @DaskParquetCache
    def _clean_data_with_timestamps(self, cache_variables=None) -> DataFrame:
        """
        Internal method that performs actual cleaning with timestamp extraction.

        Decorated with @DaskParquetCache for distributed caching.

        Args:
            cache_variables (list, optional): Variables used for cache key generation

        Returns:
            DataFrame: Cleaned Dask DataFrame with temporal features

        Implementation notes:
        - Timestamp format: "%m/%d/%Y %I:%M:%S %p" (e.g., "07/31/2019 12:41:59 PM")
        - Uses Dask's dd.to_datetime() for distributed timestamp parsing
        - Temporal features extracted via map_partitions for efficiency
        - All operations are immutable (creates new columns, doesn't modify existing)
        - Drops original position columns after extracting x_pos/y_pos
        """
        # Step 1: Select specified columns
        self.cleaned_data = self.data[self.columns]

        # Step 2: Drop rows with null values
        self.cleaned_data = self.cleaned_data.dropna()

        # OPTIMIZATION: Convert low-cardinality string columns to categorical
        if 'metadata_recordType' in self.cleaned_data.columns:
            self.cleaned_data = self.cleaned_data.assign(
                metadata_recordType=self.cleaned_data['metadata_recordType'].astype('category')
            )

        # Step 3: Parse WKT POINT to extract x_pos and y_pos
        # Uses optimized map_partitions wrapper to extract both coordinates in one pass
        self.cleaned_data = self.cleaned_data.map_partitions(
            extract_xy_coordinates,
            point_col='coreData_position',
            x_col='x_pos',
            y_col='y_pos',
            meta=self.cleaned_data._meta.assign(x_pos=0.0, y_pos=0.0)
        )

        # Step 4: Parse timestamp from string to datetime
        # Input format: "07/31/2019 12:41:59 PM" (MM/dd/yyyy hh:mm:ss a)
        # Dask's dd.to_datetime() uses pandas strptime format strings
        self.cleaned_data = self.cleaned_data.assign(
            metadata_generatedAt=dd.to_datetime(
                self.cleaned_data['metadata_generatedAt'],
                format='mixed'
            )
        )

        # Step 5: Extract temporal features using map_partitions
        # This is more efficient than applying multiple operations separately
        # because it processes all temporal features in a single pass over each partition
        def _extract_temporal_features(partition: pd.DataFrame) -> pd.DataFrame:
            """
            Extract all temporal features from metadata_generatedAt column.

            This function operates on pandas DataFrames (partitions) and uses
            pandas datetime attributes for feature extraction.

            Args:
                partition: A single partition (pandas DataFrame)

            Returns:
                pd.DataFrame: Partition with new temporal feature columns
            """
            # Extract month (1-12) - cast to int64 for compatibility
            partition['month'] = partition['metadata_generatedAt'].dt.month.astype('int64')

            # Extract day of month (1-31) - cast to int64 for compatibility
            partition['day'] = partition['metadata_generatedAt'].dt.day.astype('int64')

            # Extract year (YYYY) - cast to int64 for compatibility
            partition['year'] = partition['metadata_generatedAt'].dt.year.astype('int64')

            # Extract hour (0-23) - cast to int64 for compatibility
            partition['hour'] = partition['metadata_generatedAt'].dt.hour.astype('int64')

            # Extract minute (0-59) - cast to int64 for compatibility
            partition['minute'] = partition['metadata_generatedAt'].dt.minute.astype('int64')

            # Extract second (0-59) - cast to int64 for compatibility
            partition['second'] = partition['metadata_generatedAt'].dt.second.astype('int64')

            # Extract AM/PM indicator (0 for AM, 1 for PM)
            # Hour < 12 means AM (0), hour >= 12 means PM (1)
            partition['pm'] = (partition['metadata_generatedAt'].dt.hour >= 12).astype('int64')

            return partition

        # Create meta DataFrame with expected columns and dtypes
        meta = self.cleaned_data._meta.copy()
        meta['month'] = pd.Series(dtype='int64')
        meta['day'] = pd.Series(dtype='int64')
        meta['year'] = pd.Series(dtype='int64')
        meta['hour'] = pd.Series(dtype='int64')
        meta['minute'] = pd.Series(dtype='int64')
        meta['second'] = pd.Series(dtype='int64')
        meta['pm'] = pd.Series(dtype='int64')

        # Apply temporal feature extraction to all partitions
        self.cleaned_data = self.cleaned_data.map_partitions(
            _extract_temporal_features,
            meta=meta
        )

        # Step 6: Drop original position columns (no longer needed)
        self.cleaned_data = self.cleaned_data.drop(
            columns=['coreData_position', 'coreData_position_lat', 'coreData_position_long']
        )

        # Step 7: Optionally convert to XY coordinates (distance from origin)
        if self.isXYCoords:
            self.convert_to_XY_Coordinates()

        return self.cleaned_data
