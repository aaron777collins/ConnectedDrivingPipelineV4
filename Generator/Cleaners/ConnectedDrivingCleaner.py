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
@StandardDependencyInjection
class ConnectedDrivingCleaner(IConnectedDrivingCleaner):

    def __init__(self, pathProvider: IGeneratorPathProvider, contextProvider: IGeneratorContextProvider, data=None):
        self._generatorPathProvider = pathProvider()
        self._generatorContextProvider = contextProvider()
        self.logger = Logger("ConnectedDrivingCleaner")
        # Make sure it is unique to the option chosen (timestamps or no timestamps AND isXYCoords or not)
        self.cleandatapath = self._generatorPathProvider.getPathWithModelName("ConnectedDrivingCleaner.cleandatapath")
        os.makedirs(os.path.dirname(self.cleandatapath), exist_ok=True)

        self.isXYCoords = self._generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords")

        self.data = data
        self.x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        self.y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

        if not isinstance(self.data, pd.DataFrame):
            self.logger.log("No data specified. Using the gatherer to get the data (gather_data func).")
            self.data = DataGatherer().gather_data()

    # executes the cleaning of the data and caches it
    def clean_data(self):
        self._clean_data(full_file_cache_path=self.cleandatapath)
        return self

    # caches the cleaned data
    @CSVCache
    def _clean_data(self, full_file_cache_path="REPLACE_ME"):
        self.cleaned_data = self.data[self.columns]
        self.cleaned_data = self.cleaned_data.dropna()
        self.cleaned_data["x_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[0])
        self.cleaned_data["y_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[1])
        self.cleaned_data.drop(columns=["coreData_position"], inplace=True)

        if (self.isXYCoords):
            self.convert_to_XY_Coordinates()

        return self.cleaned_data

    # executes the cleaning of the data with timestamps and caches it
    def clean_data_with_timestamps(self):
        self._clean_data_with_timestamps(full_file_cache_path=self.cleandatapathtimestamps)
        return self

    # caches the cleaned data with timestamps
    @CSVCache
    def _clean_data_with_timestamps(self, full_file_cache_path="REPLACE_ME"):
        os.makedirs(os.path.dirname(self.cleandatapath), exist_ok=True)
        self.cleaned_data = self.data[self.columns]
        self.cleaned_data = self.cleaned_data.dropna()
        self.cleaned_data["x_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[0])
        self.cleaned_data["y_pos"] = self.cleaned_data["coreData_position"].map(lambda x: DataConverter.point_to_tuple(x)[1])
        # of format "07/31/2019 12:41:59 PM"
        # convert to datetime
        self.cleaned_data["metadata_generatedAt"] = pd.to_datetime(self.cleaned_data["metadata_generatedAt"], format="%m/%d/%Y %I:%M:%S %p")
        self.cleaned_data["month"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.month)
        self.cleaned_data["day"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.day)
        self.cleaned_data["year"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.year)
        self.cleaned_data["hour"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.hour)
        self.cleaned_data["minute"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.minute)
        self.cleaned_data["second"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: x.second)
        self.cleaned_data["pm"] = self.cleaned_data["metadata_generatedAt"].map(lambda x: 0 if x.hour < 12 else 1) # 0 if am, 1 if pm

        self.cleaned_data["metadata_generatedAt"]
        self.cleaned_data.drop(columns=["coreData_position", "coreData_position_lat", "coreData_position_long"], inplace=True)

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
