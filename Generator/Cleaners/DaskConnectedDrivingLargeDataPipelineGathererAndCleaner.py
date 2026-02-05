"""
Dask-based pipeline orchestrator for gathering and cleaning large datasets.

This replaces ConnectedDrivingLargeDataPipelineGathererAndCleaner for Dask pipelines.
Instead of using pandas DataGatherer (which loads entire 40GB CSV into memory),
this uses the DaskConnectedDrivingLargeDataCleaner which handles data gathering
via DaskDataGatherer internally, keeping everything in Dask's distributed framework.
"""

from Logger.Logger import Logger
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider


class DaskConnectedDrivingLargeDataPipelineGathererAndCleaner:
    """
    Dask-based orchestrator for the gather -> clean -> combine pipeline.
    
    Mirrors the interface of ConnectedDrivingLargeDataPipelineGathererAndCleaner
    but uses Dask internally for distributed processing.
    
    The Dask cleaner handles data gathering internally (via DaskDataGatherer),
    so we don't need to split the massive CSV with pandas first.
    """

    def __init__(self):
        self._generatorContextProvider = GeneratorContextProvider()
        self.logger = Logger("DaskPipelineGathererAndCleaner")

    def run(self):
        self.logger.log("Starting Dask-based gather and clean pipeline...")
        
        # Get the configured cleaner class (e.g., DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleanerWithFilterClass = self._generatorContextProvider.get(
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass"
        )
        
        self.logger.log(f"Using cleaner: {cleanerWithFilterClass.__name__}")
        self.dc = cleanerWithFilterClass()
        self.dc.clean_data()
        self.dc.combine_data()
        
        self.logger.log("Dask gather and clean pipeline complete.")
        return self

    def getNRows(self, n):
        """Get N rows from cleaned data (returns pandas DataFrame)."""
        return self.dc.getNRows(n)

    def getNumOfRows(self):
        """Get total number of rows in cleaned dataset."""
        return self.dc.getNumOfRows()

    def getAllRows(self):
        """
        Get all rows from cleaned dataset as a PANDAS DataFrame.
        
        Note: This triggers .compute() to convert from Dask to pandas.
        The downstream pipeline code (train/test split, ML classifiers) 
        needs pandas DataFrames, so we convert here.
        """
        dask_df = self.dc.getAllRows()
        self.logger.log("Converting Dask DataFrame to pandas...")
        pandas_df = dask_df.compute()
        self.logger.log(f"Converted {len(pandas_df):,} rows to pandas DataFrame")
        return pandas_df
