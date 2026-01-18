"""
DaskCleanerWithFilterWithinRangeXYAndDateRange - Dask implementation of CleanerWithFilterWithinRangeXYAndDateRange.

This class provides combined spatial and temporal filtering of BSM data:
- Spatial: Filters based on Euclidean (XY) distance from origin (0, 0)
- Temporal: Filters to only include data within a date range (start_date to end_date)

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
from datetime import datetime

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Helpers.DaskUDFs.GeospatialFunctions import xy_distance
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


def filter_within_xy_range_and_date_range(partition: pd.DataFrame,
                                           x_col: str,
                                           y_col: str,
                                           max_dist: float,
                                           start_day: int,
                                           start_month: int,
                                           start_year: int,
                                           end_day: int,
                                           end_month: int,
                                           end_year: int) -> pd.DataFrame:
    """
    Filter partition to include only points within Euclidean distance range from origin (0, 0)
    AND within the specified date range (inclusive).

    This function is designed to be used with map_partitions for efficient filtering.
    It's defined at module level to ensure deterministic tokenization by Dask.

    Args:
        partition: Pandas DataFrame partition
        x_col: Column name for X coordinate
        y_col: Column name for Y coordinate
        max_dist: Maximum distance from origin (0, 0)
        start_day: Start day of date range (1-31)
        start_month: Start month of date range (1-12)
        start_year: Start year of date range (e.g., 2021)
        end_day: End day of date range (1-31)
        end_month: End month of date range (1-12)
        end_year: End year of date range (e.g., 2021)

    Returns:
        Filtered partition containing only points within range and within the date range
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

    # Filter by temporal criteria (date range)
    # Create datetime objects for comparison
    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)

    # Create datetime column from day/month/year columns for comparison
    partition['temp_date'] = pd.to_datetime(
        partition[['year', 'month', 'day']].rename(columns={'year': 'year', 'month': 'month', 'day': 'day'}),
        errors='coerce'
    )

    # Filter rows within the date range (inclusive)
    partition = partition[(partition['temp_date'] >= start_date) & (partition['temp_date'] <= end_date)]

    # Drop the temporary date column
    partition = partition.drop('temp_date', axis=1)

    return partition


class DaskCleanerWithFilterWithinRangeXYAndDateRange(DaskConnectedDrivingLargeDataCleaner):
    """
    Dask-based cleaner with combined spatial (XY distance) and temporal (date range) filtering.

    This cleaner filters BSM data to include only points that:
    1. Are within a specified Euclidean distance from the origin (0, 0)
    2. Fall within the specified date range (start_date to end_date, inclusive)

    Configuration (from generatorContextProvider):
        - DaskConnectedDrivingLargeDataCleaner.max_dist: Maximum distance from origin (0, 0)
        - CleanerWithFilterWithinRangeXYAndDateRange.start_day: Start day of range (1-31)
        - CleanerWithFilterWithinRangeXYAndDateRange.start_month: Start month of range (1-12)
        - CleanerWithFilterWithinRangeXYAndDateRange.start_year: Start year of range (e.g., 2021)
        - CleanerWithFilterWithinRangeXYAndDateRange.end_day: End day of range (1-31)
        - CleanerWithFilterWithinRangeXYAndDateRange.end_month: End month of range (1-12)
        - CleanerWithFilterWithinRangeXYAndDateRange.end_year: End year of range (e.g., 2021)

    Usage:
        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange()
        cleaner.clean_data()
        filtered_df = cleaner.within_rangeXY_and_date_range(cleaner.getAllRows())
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskCleanerWithFilterWithinRangeXYAndDateRange.

        Args:
            generatorPathProvider: Provides file system paths
            initialGathererPathProvider: Provides initial gatherer paths
            generatorContextProvider: Provides configuration context
        """
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.start_day = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.start_day')
        self.start_month = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.start_month')
        self.start_year = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.start_year')
        self.end_day = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.end_day')
        self.end_month = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.end_month')
        self.end_year = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDateRange.end_year')
        self.logger = Logger("DaskCleanerWithFilterWithinRangeXYAndDateRange")

    def within_rangeXY_and_date_range(self, df: DataFrame) -> DataFrame:
        """
        Filter DataFrame to include only points within max_dist of origin (0, 0)
        AND within the specified date range.

        This method:
        1. Calculates Euclidean distance from each point to the origin (0, 0)
        2. Filters rows where distance <= max_dist
        3. Filters rows within the specified date range (inclusive)
        4. Drops the temporary 'distance' and 'temp_date' columns
        5. Returns the filtered DataFrame

        Args:
            df (DataFrame): Dask DataFrame with x_col, y_col, day, month, year columns

        Returns:
            DataFrame: Filtered Dask DataFrame containing only points within range and date range
        """
        self.logger.info(
            f"Applying combined spatial-temporal filter: center=(0, 0), "
            f"max_dist={self.max_dist}, date_range={self.start_year}-{self.start_month:02d}-{self.start_day:02d} to "
            f"{self.end_year}-{self.end_month:02d}-{self.end_day:02d}"
        )

        # Use map_partitions for efficient filtering
        # filter_within_xy_range_and_date_range is defined at module level for deterministic tokenization
        df_filtered = df.map_partitions(
            filter_within_xy_range_and_date_range,
            x_col=self.x_col,
            y_col=self.y_col,
            max_dist=self.max_dist,
            start_day=self.start_day,
            start_month=self.start_month,
            start_year=self.start_year,
            end_day=self.end_day,
            end_month=self.end_month,
            end_year=self.end_year,
            meta=df
        )

        # Log filtering results
        self.logger.info("Combined spatial-temporal filtering complete")

        return df_filtered
