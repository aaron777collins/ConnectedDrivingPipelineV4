"""
DaskCleanerWithFilterWithinRange - Dask implementation of CleanerWithFilterWithinRange.

This class provides spatial filtering of BSM data based on geodesic distance from a center point.
It filters out all points that are beyond a maximum distance from (x_pos, y_pos) using the WGS84
ellipsoid for accurate distance calculation.

Key differences from pandas version:
- Inherits from DaskConnectedDrivingLargeDataCleaner instead of ConnectedDrivingLargeDataCleaner
- Uses dask.dataframe.DataFrame instead of pandas.DataFrame
- Uses DaskParquetCache instead of CSVCache
- Uses geodesic_distance UDF for vectorized distance calculation
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
from Helpers.DaskUDFs.GeospatialFunctions import geodesic_distance
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


def filter_within_geodesic_range(partition: pd.DataFrame,
                                  x_col: str,
                                  y_col: str,
                                  center_x: float,
                                  center_y: float,
                                  max_dist: float) -> pd.DataFrame:
    """
    Filter partition to include only points within geodesic distance range.

    This function is designed to be used with map_partitions for efficient filtering.
    It's defined at module level to ensure deterministic tokenization by Dask.

    Args:
        partition: Pandas DataFrame partition
        x_col: Column name for longitude (X coordinate)
        y_col: Column name for latitude (Y coordinate)
        center_x: Center point longitude
        center_y: Center point latitude
        max_dist: Maximum distance in meters

    Returns:
        Filtered partition containing only points within range
    """
    if len(partition) == 0:
        return partition

    # Calculate geodesic distance for each row
    partition = partition.copy()
    partition['distance'] = partition.apply(
        lambda row: geodesic_distance(
            row[y_col],  # lat1 (current point latitude)
            row[x_col],  # lon1 (current point longitude)
            center_y,    # lat2 (center point latitude)
            center_x     # lon2 (center point longitude)
        ),
        axis=1
    )

    # Filter points within range
    partition = partition[partition['distance'] <= max_dist]

    # Drop the distance column
    partition = partition.drop('distance', axis=1)

    return partition


class DaskCleanerWithFilterWithinRange(DaskConnectedDrivingLargeDataCleaner):
    """
    Dask-based cleaner with geodesic distance filtering.

    This cleaner filters BSM data to include only points within a specified geodesic distance
    from a center point (x_pos, y_pos). Uses the WGS84 ellipsoid for accurate distance
    calculation, making it suitable for large geographic areas where Euclidean distance
    would be inaccurate.

    Configuration (from generatorContextProvider):
        - ConnectedDrivingCleaner.x_pos: Center point longitude
        - ConnectedDrivingCleaner.y_pos: Center point latitude
        - ConnectedDrivingLargeDataCleaner.max_dist: Maximum distance in meters

    Usage:
        cleaner = DaskCleanerWithFilterWithinRange()
        cleaner.clean_data()
        filtered_df = cleaner.within_range(cleaner.getAllRows())
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskCleanerWithFilterWithinRange.

        Args:
            generatorPathProvider: Provides file system paths
            initialGathererPathProvider: Provides initial gatherer paths
            generatorContextProvider: Provides configuration context
        """
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("DaskCleanerWithFilterWithinRange")

    def within_range(self, df: DataFrame) -> DataFrame:
        """
        Filter DataFrame to include only points within max_dist of (x_pos, y_pos).

        This method:
        1. Calculates geodesic distance from each point to the center point
        2. Filters rows where distance <= max_dist
        3. Drops the temporary 'distance' column
        4. Returns the filtered DataFrame

        Args:
            df (DataFrame): Dask DataFrame with x_col and y_col columns

        Returns:
            DataFrame: Filtered Dask DataFrame containing only points within range
        """
        self.logger.info(
            f"Applying geodesic distance filter: center=({self.x_pos}, {self.y_pos}), "
            f"max_dist={self.max_dist}m"
        )

        # Use map_partitions for efficient filtering
        # filter_within_geodesic_range is defined at module level for deterministic tokenization
        df_filtered = df.map_partitions(
            filter_within_geodesic_range,
            x_col=self.x_col,
            y_col=self.y_col,
            center_x=self.x_pos,
            center_y=self.y_pos,
            max_dist=self.max_dist,
            meta=df
        )

        # Log filtering results
        self.logger.info("Geodesic distance filtering complete")

        return df_filtered
