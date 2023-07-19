import random
import pandas as pd
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Helpers.MathHelper import MathHelper
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class StandardPositionFromOriginAttacker(ConnectedDrivingAttacker):
    @StandardDependencyInjection
    def __init__(self, data, id: str, pathProvider: IGeneratorPathProvider, generatorContextProvider: IGeneratorContextProvider):
        super().__init__(data, id)
        self._generatorContextProvider = generatorContextProvider()


    # adds a constant positional offset attack to the core data
    # Affected columns: coreData_position_lat,coreData_position_long
    # direction_angle is north at 0 (- is to the west, + is to the east)
    def add_attacks_positional_override_const(self, direction_angle=45, distance_meters=50):
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_override_const(direction_angle, distance_meters, cache_variables=[
            self.__class__.__name__, direction_angle, distance_meters, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id
        ]
        )

        return self

    @CSVCache
    def _add_attacks_positional_override_const(self, direction_angle=45, distance_meters=50, cache_variables=[
     "REPLACE_ME_WITH_ALL_VARIABLES_THAT_CHANGE_THE_OUTPUT_OF_THIS_FUNCTION"
    ]) -> pd.DataFrame:
        # Applying the attack to the data when the isAttacker column is 1

        # Checking if the row is an attacker
        # applying the attack function to each row
        self.data = self.data.apply(lambda row: self.positional_override_const_attack(row, direction_angle, distance_meters), axis=1)

        return self.data

    def positional_override_const_attack(self, row, direction_angle, distance_meters):
        # Checking if the row is not an attacker
        if row["isAttacker"] == 0:
            return row # if not an attacker, return the row as is


        # checking if the coordinates are in XY format
        if self.isXYCoords:
            # add attack with XY coordinates
            # calculating positional offset based on direction angle and distance
            newX, newY = MathHelper.direction_and_dist_to_XY(0, 0, direction_angle, distance_meters)

            row[self.x_col] = newX
            row[self.y_col] = newY
        else:
            # getting origin point from generator context
            x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
            y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")
            # calculating positional offset based on direction angle and distance
            newLat, newLong = MathHelper.direction_and_dist_to_lat_long_offset(y_pos, x_pos, direction_angle, distance_meters)
            row[self.pos_lat_col] = newLat
            row[self.pos_long_col] = newLong
        return row

    def add_attacks_positional_override_rand(self, min_dist=25, max_dist = 250):
        clean_params = self._generatorContextProvider.get("ConnectedDrivingCleaner.cleanParams")
        # the function name is already part of the cache_variables, so we don't need to add it here
        self._add_attacks_positional_override_rand(min_dist, max_dist, cache_variables=[
            self.__class__.__name__, min_dist, max_dist, self.isXYCoords, self.attack_ratio, self.SEED,
            clean_params, self.id
        ]
        )

        return self

    @CSVCache
    def _add_attacks_positional_override_rand(self, min_dist=25, max_dist = 250, cache_variables=[
            "REPLACE_ME_WITH_ALL_VARIABLES_THAT_CHANGE_THE_OUTPUT_OF_THIS_FUNCTION"
        ]
    ) -> pd.DataFrame:
        # similar to the const attack, but the distance and direction is random

        self.data = self.data.apply(lambda row: self.positional_override_rand_attack(row, min_dist, max_dist), axis=1)

        return self.data

    def positional_override_rand_attack(self, row, min_dist, max_dist):
        # checking if the row is not an attacker
        if row["isAttacker"] == 0:
            return row # if not an attacker, return the row as is

        if self.isXYCoords:
            # add attack with XY coordinates
            # calculating the positional offset based on a random direction and distance
            newX, newY = MathHelper.direction_and_dist_to_XY(0, 0, random.randint(0, 360), random.randint(min_dist, max_dist))

            row[self.x_col] = newX
            row[self.y_col] = newY

        else:
            x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
            y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

            # calculating the positional offset based on a random direction and distance
            newLat, newLong = MathHelper.direction_and_dist_to_lat_long_offset(y_pos, x_pos, random.randint(0, 360), random.randint(min_dist, max_dist))

            row[self.pos_lat_col] = newLat
            row[self.pos_long_col] = newLong

        return row
