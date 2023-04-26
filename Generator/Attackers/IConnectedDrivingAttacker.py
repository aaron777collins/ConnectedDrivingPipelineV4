
from abc import ABC, abstractmethod

class IConnectedDrivingAttacker(ABC):

    # Must be run after the data is clean to get a proper result.
    # This function returns the list of unique IDs from the coreData_id column
    @abstractmethod
    def getUniqueIDsFromCleanData(self):
        pass

    # returns the data with the new attackers
    # It requires having the modified_data_path exist and the uniqueIDs column
    @abstractmethod
    def add_attackers(self):
        pass

    # returns the data with the new attackers
    # It requires having the modified_data_path exist and the uniqueIDs column
    # this one is random and doesn't specify the attackers by ID
    def add_rand_attackers(self):
        pass

    # adds a constant positional offset attack to the core data
    # Affected columns: coreData_position_lat,coreData_position_long
    # direction_angle is north at 0 (- is to the west, + is to the east)
    def add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50):
        pass

    # calculates the attack for the row
    def positional_offset_const_attack(self, row, direction_angle, distance_meters):
        pass

    # adds a random positional offset attack to the core data
    def add_attacks_positional_offset_rand(self, min_dist=25, max_dist = 250):
        pass

    # calculates the attack for the row
    def positional_offset_rand_attack(self, row, min_dist, max_dist):
        pass

    # returns the data
    def get_data(self):
        pass
