from abc import ABC, abstractmethod

class IDataGatherer(ABC):

    @abstractmethod
    def gather_data(self):
        pass

    @abstractmethod
    def get_gathered_data(self):
        pass
