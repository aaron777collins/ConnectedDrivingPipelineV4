import random
import pandas as pd
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Helpers.MathHelper import MathHelper
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class StandardPositionalOffsetAttacker(ConnectedDrivingAttacker):
    @StandardDependencyInjection
    def __init__(self, data, id: str, pathProvider: IGeneratorPathProvider, generatorContextProvider: IGeneratorContextProvider):
        super().__init__(data, id)

    def add_attacks_positional_offset_const_per_id_with_random_direction(self, min_dist=25, max_dist=250, additionalID="DEFAULT_ID"):
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        # the function name is already part of the cache_variables, so we don't need to add it here

        self._add_attacks_positional_offset_const_per_id_with_random_direction(min_dist, max_dist, additionalID=additionalID, cache_variables=[
            self.__class__.__name__, min_dist, max_dist, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id, additionalID
        ])

        return self


    @CSVCache
    def _add_attacks_positional_offset_const_per_id_with_random_direction(self, min_dist=25, max_dist=250, additionalID="DEFAULT_ID", cache_variables=[
            "REPLACE_ME_WITH_ALL_VARIABLES_THAT_CHANGE_THE_OUTPUT_OF_THIS_FUNCTION"
        ]
    ) -> pd.DataFrame:
        # similar to the const attack, but the distance and direction is random per ID
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")

        # we are going to use the service provider to store the ID with its respective direction and distance
        # this way we can make sure every car is giving its own constant attack but in a random direction
        configStr = '-'.join([
            self.__class__.__name__, min_dist, max_dist, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id, 'add_attacks_positional_offset_const_per_id_with_random_direction', additionalID
        ])
        lookupDictStr = f"ConnectedDrivingAttacker[{configStr}].add_attacks_positional_offset_const_per_id_with_random_direction_DICT"
        lookupDict = self._generatorContextProvider.get(lookupDictStr, dict())

        self.data = self.data.apply(lambda row: self.positional_offset_const_attack_per_id_with_random_direction(row, min_dist, max_dist, lookupDict), axis=1)

        # Note that we don't need to update the lookupDict in the service provider,
        # because it is passed by reference and thus the changes we made to it will be
        # reflected in the service provider

        return self.data

    def positional_offset_const_attack_per_id_with_random_direction(self, row, min_dist, max_dist, lookupDict):
        # Checking if the row is not an attacker
        if row["isAttacker"] == 0:
            return row # if not an attacker, return the row as is

        # lookup car ID in the dictionary and get the direction and distance
        if row["coreData_id"] not in lookupDict:
            lookupDict[row["coreData_id"]] = dict()
            lookupDict[row["coreData_id"]]["direction"] = random.randint(0, 360)
            lookupDict[row["coreData_id"]]["distance"] = random.randint(min_dist, max_dist)

        # set the direction and distance
        direction_angle = lookupDict[row["coreData_id"]]["direction"]
        distance_meters = lookupDict[row["coreData_id"]]["distance"]

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

        # Note that we don't need to return the lookupDict, because it is passed by reference
        # and thus the changes we made to it will be reflected in the service provider

        return row





    # adds a constant positional offset attack to the core data
    # Affected columns: coreData_position_lat,coreData_position_long
    # direction_angle is north at 0 (- is to the west, + is to the east)
    def add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50):
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_offset_const(direction_angle, distance_meters, cache_variables=[
            self.__class__.__name__, direction_angle, distance_meters, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id
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
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_offset_rand(min_dist, max_dist, cache_variables=[
            self.__class__.__name__, min_dist, max_dist, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id
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
