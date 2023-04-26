
from abc import ABC, abstractmethod

class IConnectedDrivingCleaner(ABC):

    @abstractmethod
    def clean_data(self):
        pass

    @abstractmethod
    def clean_data_with_timestamps(self):
        pass

    @abstractmethod
    def convert_to_XY_Coordinates(self):
        pass

    @abstractmethod
    def get_cleaned_data(self):
        pass
