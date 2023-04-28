# Seperates the isAttacker column from the data (and cleans it) and returns the cleaned data and the isAttacker column
import os
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection

import pandas as pd
from sklearn.model_selection import train_test_split
from Logger.Logger import Logger
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

class MConnectedDrivingDataCleaner:
    @StandardDependencyInjection
    def __init__(self, data, pathprovider: IMLPathProvider, contextprovider: IMLContextProvider):
        self._MLPathProvider = pathprovider()
        self._MLContextprovider = contextprovider()
        self.logger = Logger("MConnectedDrivingDataCleaner")

        self.data = data

        self.cleandatapath = self._MLPathProvider.getPathWithModelName("MConnectedDrivingDataCleaner.cleandatapath")
        self.columns = self._MLContextprovider.get("MConnectedDrivingDataCleaner.columns")


    def clean_data(self):
        self.cleaned_data = self._clean_data(full_file_cache_path=self.cleandatapath)
        return self

    @CSVCache
    def _clean_data(self, full_file_cache_path="REPLACE_ME") -> pd.DataFrame:
        os.makedirs(os.path.dirname(self.cleandatapath), exist_ok=True)

        def convert_large_hex_str_to_hex(num):
            # account for str with decimal point
            if "." in num:
                num = num.split(".")[0]
            # convert to hex
            num = int(num, 16)

            return num

        # clean the data
        self.logger.log("Cleaning data...")
        self.cleaned_data = self.data[self.columns]
        # convert the coreData_id from hexadecimal to decimal
        self.cleaned_data["coreData_id"] = self.cleaned_data["coreData_id"].map(lambda x: convert_large_hex_str_to_hex(x))
        return self.cleaned_data

    def get_cleaned_data(self):
        return self.cleaned_data
