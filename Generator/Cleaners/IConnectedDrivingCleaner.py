
from abc import ABC, abstractmethod

from ClassTypes.SingletonABCMeta import SingletonABCMeta

class IConnectedDrivingCleaner(ABC, metaclass=SingletonABCMeta):

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
