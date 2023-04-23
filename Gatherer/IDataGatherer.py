from abc import ABC, abstractmethod

class IDataGatherer(ABC):

    @abstractmethod
    def gatherData(self):
        pass

    @abstractmethod
    def getGatheredData(self):
        pass
