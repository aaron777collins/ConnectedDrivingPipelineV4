"""
DaskCleanerWithFilterWithinRangeXY - Dask implementation of CleanerWithFilterWithinRangeXY.

This class provides spatial filtering of BSM data based on Euclidean (XY) distance from the origin (0, 0).
It filters out all points that are beyond a maximum distance from the center using simple Euclidean
distance calculation.

Key differences from pandas version:
- Inherits from DaskConnectedDrivingLargeDataCleaner instead of ConnectedDrivingLargeDataCleaner
- Uses dask.dataframe.DataFrame instead of pandas.DataFrame
- Uses DaskParquetCache instead of CSVCache
- Uses xy_distance UDF for vectorized distance calculation
- Uses map_partitions for efficient filtering
- No .copy() needed (Dask DataFrames are immutable)
- No inplace=True operations (Dask doesn't support them)
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


def filter_within_xy_range(partition: pd.DataFrame,
                           x_col: str,
                           y_col: str,
                           max_dist: float) -> pd.DataFrame:
    """
    Filter partition to include only points within Euclidean distance range from origin (0, 0).

    This function is designed to be used with map_partitions for efficient filtering.
    It's defined at module level to ensure deterministic tokenization by Dask.

    Args:
        partition: Pandas DataFrame partition
        x_col: Column name for X coordinate
        y_col: Column name for Y coordinate
        max_dist: Maximum distance from origin (0, 0)

    Returns:
        Filtered partition containing only points within range
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

    # Filter points within range
    partition = partition[partition['distance'] <= max_dist]

    # Drop the distance column
    partition = partition.drop('distance', axis=1)

    return partition


class DaskCleanerWithFilterWithinRangeXY(DaskConnectedDrivingLargeDataCleaner):
    """
    Dask-based cleaner with Euclidean (XY) distance filtering from origin.

    This cleaner filters BSM data to include only points within a specified Euclidean distance
    from the origin (0, 0). Uses simple XY distance calculation, making it suitable for
    scenarios where the coordinates are already in a projected coordinate system or when
    Euclidean approximation is acceptable.

    Configuration (from generatorContextProvider):
        - DaskConnectedDrivingLargeDataCleaner.max_dist: Maximum distance from origin (0, 0)

    Usage:
        cleaner = DaskCleanerWithFilterWithinRangeXY()
        cleaner.clean_data()
        filtered_df = cleaner.within_rangeXY(cleaner.getAllRows())
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskCleanerWithFilterWithinRangeXY.

        Args:
            generatorPathProvider: Provides file system paths
            initialGathererPathProvider: Provides initial gatherer paths
            generatorContextProvider: Provides configuration context
        """
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("DaskCleanerWithFilterWithinRangeXY")

    def within_rangeXY(self, df: DataFrame) -> DataFrame:
        """
        Filter DataFrame to include only points within max_dist of origin (0, 0).

        This method:
        1. Calculates Euclidean distance from each point to the origin (0, 0)
        2. Filters rows where distance <= max_dist
        3. Drops the temporary 'distance' column
        4. Returns the filtered DataFrame

        Args:
            df (DataFrame): Dask DataFrame with x_col and y_col columns

        Returns:
            DataFrame: Filtered Dask DataFrame containing only points within range
        """
        self.logger.info(
            f"Applying Euclidean distance filter: center=(0, 0), "
            f"max_dist={self.max_dist}"
        )

        # Use map_partitions for efficient filtering
        # filter_within_xy_range is defined at module level for deterministic tokenization
        df_filtered = df.map_partitions(
            filter_within_xy_range,
            x_col=self.x_col,
            y_col=self.y_col,
            max_dist=self.max_dist,
            meta=df
        )

        # Log filtering results
        self.logger.info("Euclidean distance filtering complete")

        return df_filtered
