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
class ConnectedDrivingAttacker(IConnectedDrivingAttacker):

    @StandardDependencyInjection
    def __init__(self, data, id: str, pathProvider: IGeneratorPathProvider, generatorContextProvider: IGeneratorContextProvider):

        self.id = id

        self._pathprovider = pathProvider()
        self._generatorContextProvider = generatorContextProvider()
        self.logger = Logger("ConnectedDrivingAttacker" + id)

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

    def get_data(self):
        return self.data
