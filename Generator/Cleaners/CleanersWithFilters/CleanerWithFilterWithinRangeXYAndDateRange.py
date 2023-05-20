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

class CleanerWithFilterWithinRangeXYAndDateRange(ConnectedDrivingLargeDataCleaner):

    @StandardDependencyInjection
    def __init__(self, generatorPathProvider: IGeneratorPathProvider, initialGathererPathProvider: IInitialGathererPathProvider, generatorContextProvider: IGeneratorContextProvider):
        super().__init__()
        self._generatorContextProvider = generatorContextProvider()
        self.startday = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.startday')
        self.startmonth = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.startmonth')
        self.startyear = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.startyear')
        self.endday = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.endday')
        self.endmonth = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.endmonth')
        self.endyear = self._generatorContextProvider.get('CleanerWithFilterWithinRangeXYAndDay.endyear')

    def within_rangeXY_and_date_range(self, df: pd.DataFrame):

        # assume (0, 0) is the center of the map
        x_pos = 0
        y_pos = 0
        # calculate the distance between each point and (x_pos, y_pos)
        df = df.copy()
        df['distance'] = df.apply(lambda row: MathHelper.dist_between_two_pointsXY(row[self.x_col], row[self.y_col], x_pos, y_pos), axis=1)
        # filter out points that are outside the max distance
        df = df[df['distance'] <= self.max_dist]
        # drop the 'distance' column
        df.drop('distance', axis=1, inplace=True)

        # filter so that it only has rows with the specified day, month, and year
        df['matchesday'] = df.apply(lambda row: row['day'] >= self.startday and row['day'] <= self.endday, axis=1)
        df['matchesmonth'] = df.apply(lambda row: row['month'] >= self.startmonth and row['month'] <= self.endmonth, axis=1)
        df['matchesyear'] = df.apply(lambda row: row['year'] >= self.startyear and row['year'] <= self.endyear, axis=1)

        df['matchesdaymonthyear'] = df.apply(lambda row: row['matchesday'] and row['matchesmonth'] and row['matchesyear'], axis=1)

        # remove the rows that don't match the day, month, and year
        df = df[df['matchesdaymonthyear'] == True]

        # drop the columns that were used to filter
        df.drop(['matchesday', 'matchesmonth', 'matchesyear', 'matchesdaymonthyear'], axis=1, inplace=True)

        return df
