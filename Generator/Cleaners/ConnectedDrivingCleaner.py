import os

import pandas as pd
from sklearn.model_selection import train_test_split
from Decorators.CSVCache import CSVCache
from Decorators.FileCache import FileCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.DataGatherer import DataGatherer

from Generator.Cleaners.IConnectedDrivingCleaner import IConnectedDrivingCleaner
from Helpers.DataConverter import DataConverter
from Helpers.MathHelper import MathHelper
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider

import os.path as path


# Dependency injects the providers (make sure they are the last arguments but before kwargs)
class ConnectedDrivingCleaner(IConnectedDrivingCleaner):

    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider, contextProvider: IGeneratorContextProvider, data=None, filename=None):
        self._generatorPathProvider = pathProvider()
        self._generatorContextProvider = contextProvider()
        self.logger = Logger("ConnectedDrivingCleaner")
        # Make sure it is unique to the option chosen (timestamps or no timestamps AND isXYCoords or not)

        # cleaning params to be used in the cache
        self.clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        self.filename = filename
        if (self.filename is None):
            # defaults the filename to clean{numrows}.csv
            # This ensures that even in the non-large data cleaner, the filename is unique with the number of rows in the data
            numrows = self._generatorContextProvider.get("DataGatherer.numrows")
            self.filename = self._generatorContextProvider.get("ConnectedDrivingCleaner.filename", f"clean{numrows}.csv")

        self.isXYCoords = self._generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords")

        self.columns = self._generatorContextProvider.get("ConnectedDrivingCleaner.columns")

        self.data = data
        self.x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        self.y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

        self.shouldGatherAutomatically = self._generatorContextProvider.get("ConnectedDrivingCleaner.shouldGatherAutomatically")

        if not isinstance(self.data, pd.DataFrame) and self.shouldGatherAutomatically:
            self.logger.log("No data specified. Using the gatherer to get the data (gather_data func).")
            self.data = DataGatherer().gather_data()
        elif not isinstance(self.data, pd.DataFrame):
            raise self.logger.log("No data specified. You may want to specify data or set ConnectedDrivingCleaner.shouldGatherAutomatically to True.")

    # executes the cleaning of the data and caches it
    def clean_data(self):
        self.cleaned_data = self._clean_data(cache_variables=[
            self.__class__.__name__, self.isXYCoords,
            self.clean_params, self.filename, self.x_pos, self.y_pos
        ])
        return self

    # caches the cleaned data
    @CSVCache
    def _clean_data(self, cache_variables=["REPLACE_ME"]) -> pd.DataFrame:
        self.cleaned_data = self.data[self.columns]
        self.cleaned_data = self.cleaned_data.dropna()
        self.cleaned_data["x_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[0])
        self.cleaned_data["y_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[1])
        self.cleaned_data.drop(columns=["coreData_position"], inplace=True)

        if (self.isXYCoords):
            self.convert_to_XY_Coordinates()

        return self.cleaned_data

    # too small of a change to be worth caching
    def convert_to_XY_Coordinates(self):
        self.cleaned_data["x_pos"] = self.cleaned_data["x_pos"].map(lambda x: MathHelper.dist_between_two_points(x, self.y_pos, self.x_pos, self.y_pos))
        self.cleaned_data["y_pos"] = self.cleaned_data["y_pos"].map(lambda y: MathHelper.dist_between_two_points(self.x_pos, y, self.x_pos, self.y_pos))
        return self

    # returns the cleaned data
    def get_cleaned_data(self):
        return self.cleaned_data
