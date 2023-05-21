from Decorators.StandardDependencyInjection import StandardDependencyInjection
import os
from pandas import DataFrame
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

class ConnectedDrivingLargeDataPipelineGathererAndCleaner:

    @StandardDependencyInjection
    def __init__(self, generatorContextProvider: IGeneratorContextProvider):
        self._generatorContextProvider = generatorContextProvider()

        self.logger = Logger("ConnectedDrivingLargeDataPipelineGathererAndCleaner")

    def run(self):

        self.dg = DataGatherer()
        self.dg.split_large_data()

        self.cleanerWithFilter = self._generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass")

        self.dc = self.cleanerWithFilter()
        self.dc.clean_data()
        self.dc.combine_data()
        return self



    def getNRows(self, n):
        # read from combined data
        return self.dc.getNumRows(n)

    def getNumOfRows(self):
        return self.dc.getNumOfRows()

    def getAllRows(self):
        return self.dc.getAllRows()
