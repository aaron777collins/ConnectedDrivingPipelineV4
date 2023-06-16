import glob
import os

import pandas as pd
from sklearn.model_selection import train_test_split
from Decorators.CSVCache import CSVCache
from Decorators.FileCache import FileCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner

from Generator.Cleaners.IConnectedDrivingCleaner import IConnectedDrivingCleaner
from Helpers.MathHelper import MathHelper
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider


import os.path as path

from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider

class CleanerWithPassthroughFilter(ConnectedDrivingLargeDataCleaner):

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider, initialGathererPathProvider: IInitialGathererPathProvider, generatorContextProvider: IGeneratorContextProvider):
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()

    def passthrough(self, df: pd.DataFrame):

        return df
