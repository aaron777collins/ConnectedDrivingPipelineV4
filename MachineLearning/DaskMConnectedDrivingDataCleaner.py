"""
DaskMConnectedDrivingDataCleaner - Dask implementation of MConnectedDrivingDataCleaner.

This class prepares cleaned BSM data for ML model training by:
1. Selecting relevant feature columns
2. Converting hexadecimal coreData_id to decimal format

Key differences from pandas version:
- Uses DaskParquetCache instead of CSVCache for distributed caching
- Uses Dask UDF (hex_to_decimal) instead of pandas .map()
- Returns Dask DataFrame (lazy evaluation)
- Compatible with sklearn (must compute() before training)

Usage:
    cleaner = DaskMConnectedDrivingDataCleaner(data=bsm_df, suffixName="_model1")
    cleaner.clean_data()
    cleaned_df = cleaner.get_cleaned_data()

    # For ML training, compute to pandas first:
    X_train_pandas = cleaned_df.compute()
"""

import os
import dask.dataframe as dd
from dask.dataframe import DataFrame

from Decorators.DaskParquetCache import DaskParquetCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Helpers.DaskUDFs import hex_to_decimal
from Logger.Logger import Logger
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider


class DaskMConnectedDrivingDataCleaner:
    """
    Dask-based ML data cleaner for Connected Driving BSM data.

    This cleaner prepares data for ML model training by:
    - Selecting feature columns specified in configuration
    - Converting coreData_id from hexadecimal to decimal format
    - Caching results as Parquet files for faster reloading

    The cleaned data can be used for training sklearn classifiers (after compute()).
    """

    @StandardDependencyInjection
    def __init__(self, data, suffixName, pathprovider: IMLPathProvider, contextprovider: IMLContextProvider):
        """
        Initialize DaskMConnectedDrivingDataCleaner.

        Args:
            data (DataFrame): Dask DataFrame with BSM data (from attackers or cleaners)
            suffixName (str): Suffix for cache path (e.g., "_model1", "_rf_100k")
            pathprovider (IMLPathProvider): Provides ML cache paths
            contextprovider (IMLContextProvider): Provides ML configuration (columns)
        """
        self._MLPathProvider = pathprovider()
        self._MLContextprovider = contextprovider()
        self.suffixName = suffixName
        self.logger = Logger(f"DaskMConnectedDrivingDataCleaner{self.suffixName}")

        self.data = data

        # Get cache path from path provider
        self.cleandatapath = self._MLPathProvider.getPathWithModelName(
            f"MConnectedDrivingDataCleaner.cleandatapath{self.suffixName}"
        )

        # Get feature columns from context provider
        # These are the columns needed for ML training (features + isAttacker label)
        self.columns = self._MLContextprovider.get("MConnectedDrivingDataCleaner.columns")

    def clean_data(self):
        """
        Clean and prepare data for ML training.

        Returns:
            self: For method chaining
        """
        self.cleaned_data = self._clean_data(full_file_cache_path=self.cleandatapath)
        return self

    @DaskParquetCache
    def _clean_data(self, full_file_cache_path="REPLACE_ME") -> DataFrame:
        """
        Internal method to clean data with Parquet caching.

        This method:
        1. Selects feature columns specified in configuration
        2. Converts coreData_id from hex to decimal (if column exists)
        3. Caches result as Parquet for fast reloading

        Args:
            full_file_cache_path (str): Cache path (set by DaskParquetCache decorator)

        Returns:
            DataFrame: Dask DataFrame with cleaned ML features
        """
        # Create cache directory if it doesn't exist
        os.makedirs(os.path.dirname(self.cleandatapath), exist_ok=True)

        # Log cleaning operation
        self.logger.log("Cleaning data for ML training...")

        # Select feature columns from configuration
        # This typically includes features like: x_pos, y_pos, speed, heading, etc.
        # and the isAttacker label column
        self.cleaned_data = self.data[self.columns]

        # Convert coreData_id from hexadecimal to decimal (if column exists)
        # The hex_to_decimal UDF handles edge cases:
        # - Strips decimal points (e.g., "0x1a2b3c4d.0" -> "0x1a2b3c4d")
        # - Returns None for invalid hex strings
        # - Handles None/null inputs gracefully
        if "coreData_id" in self.cleaned_data.columns:
            self.logger.log("Converting coreData_id from hexadecimal to decimal...")

            # Apply hex_to_decimal UDF to coreData_id column
            # meta specifies the output dtype (int64)
            self.cleaned_data = self.cleaned_data.assign(
                coreData_id=self.cleaned_data["coreData_id"].apply(
                    hex_to_decimal,
                    meta=('coreData_id', 'i8')
                )
            )

        return self.cleaned_data

    def get_cleaned_data(self):
        """
        Get the cleaned ML training data.

        Returns:
            DataFrame: Dask DataFrame with cleaned features (lazy)

        Note:
            For ML training with sklearn, you must compute() this DataFrame:
            >>> X_train_pandas = cleaner.get_cleaned_data().compute()
            >>> model.fit(X_train_pandas, y_train)
        """
        return self.cleaned_data
