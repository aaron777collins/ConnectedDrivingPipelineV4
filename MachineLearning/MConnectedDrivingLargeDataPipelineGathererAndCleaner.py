from Decorators.StandardDependencyInjection import StandardDependencyInjection
import os
from pandas import DataFrame
from Gatherer.DataGatherer import DataGatherer
from Logger.Logger import Logger
from MachineLearning.MConnectedDrivingLargeDataCleaner import MConnectedDrivingLargeDataCleaner
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

@StandardDependencyInjection
class MConnectedDrivingLargeDataPipelineGathererAndCleaner:

    def __init__(self, pathprovider: IMLPathProvider, contextprovider: IMLContextProvider):

        self._pathprovider = pathprovider()
        self._contextprovider = contextprovider()
        self.logger = Logger("MConnectedDrivingLargeDataPipelineGathererAndCleaner")

    def run(self):

        self.dg = DataGatherer()
        self.dg.split_large_data()

        self.dc = MConnectedDrivingLargeDataCleaner()
        self.dc.clean_data()
        self.dc.combine_data()
        return self



    def getNRows(self, n):
        # read from combined data
        return self.dc.getNRows(n)
