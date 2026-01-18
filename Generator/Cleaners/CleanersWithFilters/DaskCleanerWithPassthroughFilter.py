"""
DaskCleanerWithPassthroughFilter - Dask implementation of CleanerWithPassthroughFilter.

This class provides a passthrough filter that returns the DataFrame unchanged.
It's useful for testing purposes or when no filtering is needed but the interface
requires a filter cleaner.

Key differences from pandas version:
- Inherits from DaskConnectedDrivingLargeDataCleaner instead of ConnectedDrivingLargeDataCleaner
- Uses dask.dataframe.DataFrame instead of pandas.DataFrame
- Uses DaskParquetCache instead of CSVCache
"""

import os
import os.path as path
import dask.dataframe as dd
from dask.dataframe import DataFrame

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


class DaskCleanerWithPassthroughFilter(DaskConnectedDrivingLargeDataCleaner):
    """
    Dask-based passthrough filter cleaner.

    This cleaner applies no filtering - it simply returns the input DataFrame unchanged.
    Inherits all cleaning functionality from DaskConnectedDrivingLargeDataCleaner and
    adds a trivial passthrough filter.

    Usage:
        cleaner = DaskCleanerWithPassthroughFilter()
        cleaner.clean_data()
        filtered_df = cleaner.passthrough(cleaner.get_cleaned_data())
    """

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider,
                 initialGathererPathProvider: IInitialGathererPathProvider,
                 generatorContextProvider: IGeneratorContextProvider):
        """
        Initialize DaskCleanerWithPassthroughFilter.

        Args:
            generatorPathProvider: Provides file system paths
            initialGathererPathProvider: Provides initial gatherer paths
            generatorContextProvider: Provides configuration context
        """
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("DaskCleanerWithPassthroughFilter")

    def passthrough(self, df: DataFrame) -> DataFrame:
        """
        Apply passthrough filter (identity function).

        Args:
            df (DataFrame): Dask DataFrame to filter

        Returns:
            DataFrame: The same DataFrame unchanged
        """
        self.logger.info("Applying passthrough filter (no filtering)")
        return df
