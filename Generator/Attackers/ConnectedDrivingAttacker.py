# Dependency injects the providers (make sure they are the last arguments but before kwargs)
import os
import random
import pandas as pd

from sklearn.model_selection import train_test_split
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Attackers.IConnectedDrivingAttacker import IConnectedDrivingAttacker
from Helpers.MathHelper import MathHelper
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider

# TO AVOID ERRORS, ALL FILES FROM THIS CLASS ARE AUTO_CACHED in the cache folder using the cache_variables for the
# respective cached function
@StandardDependencyInjection
class ConnectedDrivingAttacker(IConnectedDrivingAttacker):

    def __init__(self, data, pathProvider: IGeneratorPathProvider, generatorContextProvider: IGeneratorContextProvider):
        self._pathprovider = pathProvider()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("ConnectedDrivingAttacker")

        self.data = data
        self.SEED = self._generatorContextProvider.get("ConnectedDrivingAttacker.SEED")
        self.isXYCoords = self._generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords")
        self.attack_ratio = self._generatorContextProvider.get("ConnectedDrivingAttacker.attack_ratio")

        self.pos_lat_col = "y_pos"
        self.pos_long_col = "x_pos"
        self.x_col = "x_pos"
        self.y_col = "y_pos"


    # Must be run after the data is clean to get a proper result.
    # This function returns the list of unique IDs from the coreData_id column
    def getUniqueIDsFromCleanData(self):
        return self.data.coreData_id.unique()

    # returns the data with the new attackers
    # It requires having the uniqueIDs column
    # not worth caching until after the attacks are added
    def add_attackers(self):
        uniqueIDs = self.getUniqueIDsFromCleanData()

        # Splits the data into the regular cars and the new chosen attackers (5% attackers)
        regular, attackers = train_test_split(uniqueIDs, test_size=self.attack_ratio, random_state=self.SEED)

        # Adds a column called isAttacker with 0 if they are regular and 1 if they are in the attackers list
        self.data["isAttacker"] = self.data.coreData_id.apply(lambda x: 1 if x in attackers else 0)

        return self

    # returns the data with the new attackers
    # It requires having the uniqueIDs column
    # this one is random and doesn't specify the attackers by ID
    # not worth caching until after the attacks are added
    def add_rand_attackers(self):

         # Adds a column called isAttacker with 0 if they are regular and 1 if they are an attacker
        # attackers are randomly chosen for each row by the attack_ratio
        self.data["isAttacker"] = self.data.coreData_id.apply(lambda x: 1 if random.random() <= self.attack_ratio else 0)

        # ensure that the path exists
        os.makedirs(os.path.dirname(self.modified_data_path), exist_ok=True)

        return self

    # adds a constant positional offset attack to the core data
    # Affected columns: coreData_position_lat,coreData_position_long
    # direction_angle is north at 0 (- is to the west, + is to the east)
    def add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50):
        clean_func_name = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanFuncName")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_offset_const(direction_angle, distance_meters, cache_variables=[
            self.__class__.__name__, direction_angle, distance_meters, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_func_name
        ]
        )

        return self

    @CSVCache
    def _add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50, cache_variables=[
     "REPLACE_ME_WITH_ALL_VARIABLES_THAT_CHANGE_THE_OUTPUT_OF_THIS_FUNCTION"
    ]) -> pd.DataFrame:
        # Applying the attack to the data when the isAttacker column is 1

        # Checking if the row is an attacker
        # applying the attack function to each row
        self.data = self.data.apply(lambda row: self.positional_offset_const_attack(row, direction_angle, distance_meters), axis=1)

        return self.data

    def positional_offset_const_attack(self, row, direction_angle, distance_meters):
        # Checking if the row is not an attacker
        if row["isAttacker"] == 0:
            return row # if not an attacker, return the row as is


        # checking if the coordinates are in XY format
        if self.isXYCoords:
            # add attack with XY coordinates
            # calculating positional offset based on direction angle and distance
            newX, newY = MathHelper.direction_and_dist_to_XY(row[self.x_col], row[self.y_col], direction_angle, distance_meters)

            row[self.x_col] = newX
            row[self.y_col] = newY
        else:
            # calculating positional offset based on direction angle and distance
            newLat, newLong = MathHelper.direction_and_dist_to_lat_long_offset(row[self.pos_lat_col], row[self.pos_long_col], direction_angle, distance_meters)
            row[self.pos_lat_col] = newLat
            row[self.pos_long_col] = newLong
        return row

    def add_attacks_positional_offset_rand(self, min_dist=25, max_dist = 250):
        clean_func_name = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanFuncName")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_offset_rand(min_dist, max_dist, cache_variables=[
            self.__class__.__name__, min_dist, max_dist, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_func_name
        ]
        )

        return self

    @CSVCache
    def _add_attacks_positional_offset_rand(self, min_dist=25, max_dist = 250, cache_variables=[
            "REPLACE_ME_WITH_ALL_VARIABLES_THAT_CHANGE_THE_OUTPUT_OF_THIS_FUNCTION"
        ]
    ) -> pd.DataFrame:
        # similar to the const attack, but the distance and direction is random

        self.data = self.data.apply(lambda row: self.positional_offset_rand_attack(row, min_dist, max_dist), axis=1)

        return self.data

    def positional_offset_rand_attack(self, row, min_dist, max_dist):
        # checking if the row is not an attacker
        if row["isAttacker"] == 0:
            return row # if not an attacker, return the row as is

        if self.isXYCoords:
            # add attack with XY coordinates
            # calculating the positional offset based on a random direction and distance
            newX, newY = MathHelper.direction_and_dist_to_XY(row[self.x_col], row[self.y_col], random.randint(0, 360), random.randint(min_dist, max_dist))

            row[self.x_col] = newX
            row[self.y_col] = newY

        else:

            # calculating the positional offset based on a random direction and distance
            newLat, newLong = MathHelper.direction_and_dist_to_lat_long_offset(row[self.pos_lat_col], row[self.pos_long_col], random.randint(0, 360), random.randint(min_dist, max_dist))

            row[self.pos_lat_col] = newLat
            row[self.pos_long_col] = newLong

        return row

    def get_data(self):
        return self.data
