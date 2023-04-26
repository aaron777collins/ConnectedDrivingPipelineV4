from Decorators.StandardDependencyInjection import StandardDependencyInjection
import os
from pandas import DataFrame
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Logger.Logger import Logger
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

class ConnectedDrivingLargeDataPipelineGathererAndCleaner:

    def __init__(self):

        self.logger = Logger("ConnectedDrivingLargeDataPipelineGathererAndCleaner")

    def run(self):

        self.dg = DataGatherer()
        self.dg.split_large_data()

        self.dc = ConnectedDrivingLargeDataCleaner()
        self.dc.clean_data()
        self.dc.combine_data()
        return self



    def getNRows(self, n):
        # read from combined data
        return self.dc.getNRows(n)
