import glob
import os

import pandas as pd
from sklearn.model_selection import train_test_split
from Decorators.CSVCache import CSVCache
from Decorators.FileCache import FileCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner

from Generator.Cleaners.IConnectedDrivingCleaner import IConnectedDrivingCleaner
from Helpers.MathHelper import MathHelper
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider


import os.path as path

from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider


# Dependency injects the providers (make sure they are the last arguments but before kwargs)
@StandardDependencyInjection
class ConnectedDrivingLargeDataCleaner:

    def __init__(self, generatorPathProvider: IGeneratorPathProvider, initialGathererPathProvider: IInitialGathererPathProvider, contextProvider: IGeneratorContextProvider):
        self._generatorPathProvider = generatorPathProvider()
        self._initialgathererpathprovider = initialGathererPathProvider()
        self._contextprovider = contextProvider()
        self.logger = Logger("ConnectedDrivingCleaner")

        self.splitfilespath = self._initialgathererpathprovider.getPathWithModelName("DataGatherer.splitfilespath")
        self.cleanedfilespath = self._generatorPathProvider.getPathWithModelName("ConnectedDrivingLargeDataCleaner.cleanedfilespath")
        self.combinedcleandatapath = self._generatorPathProvider.getPathWithModelName("ConnectedDrivingLargeDataCleaner.combinedcleandatapath")

        os.makedirs(path.dirname(self.splitfilespath), exist_ok=True)
        os.makedirs(path.dirname(self.cleanedfilespath), exist_ok=True)
        os.makedirs(path.dirname(self.combinedcleandatapath), exist_ok=True)

        self.x_pos = self._contextprovider.get("ConnectedDrivingCleaner.x_pos")
        self.y_pos = self._contextprovider.get("ConnectedDrivingCleaner.y_pos")
        self.max_dist = self._contextprovider.get("ConnectedDrivingLargeDataCleaner.max_dist")

        self.cleanFunc = self._contextprovider.get("ConnectedDrivingLargeDataCleaner.cleanFunc")
        self.filterFunc = self._contextprovider.get("ConnectedDrivingLargeDataCleaner.filterFunc")

        self.pos_lat_col = "y_pos"
        self.pos_long_col = "x_pos"
        self.x_col = "x_pos"
        self.y_col = "y_pos"


    # executes the cleaning of the data and caches it
    def clean_data(self):

        # reads each file from the split folder and cleans them and then saves them in the cleaned split folder
        glob.glob(self.splitfilespath + "*.csv")

        # create the cleaned split folder if it doesn't exist
        os.makedirs(path.dirname(self.cleanedfilespath), exist_ok=True)

        # check if there is already a cleaned file for this model type
        if len(glob.glob(self.cleanedfilespath + "*.csv")) > 0:
            self.logger.log("Found cleaned files! Skipping regeneration.")
            return self

        for file in glob.glob(self.splitfilespath + "*.csv"):
            self.logger.log(f"Cleaning file {file}")
            df = pd.read_csv(file)
            dc = ConnectedDrivingCleaner(data=df)
            newDf = self.cleanFunc(dc).get_cleaned_data()
            if (self.filterFunction != None):
                newDf = self.filterFunction(newDf)

            # only write file if there are rows in newDf
            if len(newDf) > 0:
                newDf.to_csv(self.cleanedfilespath + path.basename(file), index=False)
            else:
                self.logger.log(f"Skipping writing file {file} because it has no rows.")

        return self

    def within_range(self, df):
        # calculate the distance between each point and (x_pos, y_pos)
        df = df.copy()
        df['distance'] = df.apply(lambda row: MathHelper.dist_between_two_points(row[self.x_col], row[self.y_col], self.x_pos, self.y_pos), axis=1)
        # filter out points that are outside the max distance
        df = df[df['distance'] <= self.max_dist]
        # drop the 'distance' column
        df.drop('distance', axis=1, inplace=True)
        return df

    def within_rangeXY(self, df):
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
        return df

    def combine_data(self):
        # search the cleaned split folder for all csv files
        # combine them into one csv file

        # make sure the combined data folder exists
        os.makedirs(path.dirname(self.combinedcleandatapath), exist_ok=True)

        # check if the combined data file already exists
        if(path.exists(self.combinedcleandatapath)):
            self.logger.log("Found combined data file! Skipping regeneration.")
            return self

        first_file = True
        with open(self.combinedcleandatapath, 'w') as outfile:
            for file in glob.glob(self.cleanedfilespath + "*.csv"):
                self.logger.log(f"Combining file {file}")
                with open(file) as infile:
                    if first_file:
                        # Write the header row for the first file
                        outfile.write(infile.readline())
                        first_file = False
                    else:
                        # Skip the header row for subsequent files
                        infile.readline()

                    outfile.write(infile.read())
