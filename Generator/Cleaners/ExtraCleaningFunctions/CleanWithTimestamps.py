import os
import pandas as pd
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Helpers.DataConverter import DataConverter
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider

class CleanWithTimestamps(ConnectedDrivingCleaner):
    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider, contextProvider: IGeneratorContextProvider, data=None, filename=None):
        # initialize base class to get the basic dependencies
        super().__init__(data=data, filename=filename)



    # executes the cleaning of the data with timestamps and caches it
    def clean_data_with_timestamps(self):
        self.cleaned_data = self._clean_data_with_timestamps(cache_variables=[
            self.__class__.__name__, self.isXYCoords,
            self.clean_func_name, self.filename, self.x_pos, self.y_pos
        ])
        return self

    # caches the cleaned data with timestamps
    @CSVCache
    def _clean_data_with_timestamps(self, cache_variables=["REPLACE_ME"]) -> pd.DataFrame:
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
