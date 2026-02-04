"""
DaskCleanerWithFilterWithinRangeXYAndDay - Dask implementation of CleanerWithFilterWithinRangeXYAndDay.

This class provides combined spatial and temporal filtering of BSM data:
- Spatial: Filters based on Euclidean (XY) distance from origin (0, 0)
- Temporal: Filters to only include data from a specific day, month, and year

Key differences from pandas version:
- Inherits from DaskConnectedDrivingLargeDataCleaner instead of ConnectedDrivingLargeDataCleaner
- Uses dask.dataframe.DataFrame instead of pandas.DataFrame
- Uses DaskParquetCache instead of CSVCache
- Uses xy_distance UDF for vectorized distance calculation
- Uses map_partitions for efficient filtering
- No .copy() needed (Dask DataFrames are immutable)
- No inplace=True operations (Dask doesn't support them)
- Combines spatial and temporal filters in a single partition operation for efficiency
"""

import os
import os.path as path
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Helpers.DaskUDFs.GeospatialFunctions import xy_distance
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


def filter_within_xy_range_and_day(partition: pd.DataFrame,
                                    x_col: str,
                                    y_col: str,
                                    max_dist: float,
                                    day: int,
                                    month: int,
                                    year: int) -> pd.DataFrame:
    """
    Filter partition to include only points within Euclidean distance range from origin (0, 0)
    AND matching the specified day, month, and year.

    This function is designed to be used with map_partitions for efficient filtering.
    It's defined at module level to ensure deterministic tokenization by Dask.

    Args:
        partition: Pandas DataFrame partition
        x_col: Column name for X coordinate
        y_col: Column name for Y coordinate
        max_dist: Maximum distance from origin (0, 0)
        day: Day of month to filter (1-31)
        month: Month to filter (1-12)
        year: Year to filter (e.g., 2021)

    Returns:
        Filtered partition containing only points within range and matching the date
    """
    if len(partition) == 0:
        return partition

    # Calculate Euclidean distance from origin (0, 0) for each row
    partition = partition.copy()
    partition['distance'] = partition.apply(
        lambda row: xy_distance(
            row[x_col],  # x1 (current point X)
            row[y_col],  # y1 (current point Y)
            0.0,         # x2 (origin X)
            0.0          # y2 (origin Y)
        ),
        axis=1
    )

    # Filter points within spatial range
    partition = partition[partition['distance'] <= max_dist]

    # Drop the distance column
    partition = partition.drop('distance', axis=1)

    # Filter by temporal criteria (day, month, year)
    # Using vectorized boolean operations for efficiency
    matches_day = partition['day'] == day
    matches_month = partition['month'] == month
    matches_year = partition['year'] == year
    matches_all = matches_day & matches_month & matches_year

    partition = partition[matches_all]

    return partition


class DaskCleanerWithFilterWithinRangeXYAndDay(DaskConnectedDrivingLargeDataCleaner):
    """
    Dask-based cleaner with combined spatial (XY distance) and temporal (day) filtering.

    This cleaner filters BSM data to include only points that:
    1. Are within a specified Euclidean distance from the origin (0, 0)
    2. Match the specified day, month, and year

    Configuration (from generatorContextProvider):
        - DaskConnectedDrivingLargeDataCleaner.max_dist: Maximum distance from origin (0, 0)
        - CleanerWithFilterWithinRangeXYAndDay.day: Day of month (1-31)
        - CleanerWithFilterWithinRangeXYAndDay.month: Month (1-12)
        - CleanerWithFilterWithinRangeXYAndDay.year: Year (e.g., 2021)

    Usage:
        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay()
        cleaner.clean_data()
        filtered_df = cleaner.within_rangeXY_and_day(cleaner.getAllRows())
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskCleanerWithFilterWithinRangeXYAndDay.

        Args:
            generatorPathProvider: Provides file system paths
            initialGathererPathProvider: Provides initial gatherer paths
            generatorContextProvider: Provides configuration context
        """
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.day = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.day')
        self.month = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.month')
        self.year = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.year')
        self.logger = Logger("DaskCleanerWithFilterWithinRangeXYAndDay")

    def within_rangeXY_and_day(self, df: DataFrame) -> DataFrame:
        """
        Filter DataFrame to include only points within max_dist of origin (0, 0)
        AND matching the specified day, month, and year.

        This method:
        1. Calculates Euclidean distance from each point to the origin (0, 0)
        2. Filters rows where distance <= max_dist
        3. Filters rows matching the specified day, month, and year
        4. Drops the temporary 'distance' column
        5. Returns the filtered DataFrame

        Args:
            df (DataFrame): Dask DataFrame with x_col, y_col, day, month, year columns

        Returns:
            DataFrame: Filtered Dask DataFrame containing only points within range and matching date
        """
        self.logger.log(
            f"Applying combined spatial-temporal filter: center=(0, 0), "
            f"max_dist={self.max_dist}, date={self.year}-{self.month:02d}-{self.day:02d}"
        )

        # Use map_partitions for efficient filtering
        # filter_within_xy_range_and_day is defined at module level for deterministic tokenization
        df_filtered = df.map_partitions(
            filter_within_xy_range_and_day,
            x_col=self.x_col,
            y_col=self.y_col,
            max_dist=self.max_dist,
            day=self.day,
            month=self.month,
            year=self.year,
            meta=df
        )

        # Log filtering results
        self.logger.log("Combined spatial-temporal filtering complete")

        return df_filtered
