from abc import ABC, abstractmethod

from ClassTypes.SingletonABCMeta import SingletonABCMeta

class IPathProvider(ABC, metaclass=SingletonABCMeta):

    @abstractmethod
    def getPaths(self):
        pass

    @abstractmethod
    def setPaths(self, path):
        pass

    @abstractmethod
    def addPath(self, path):
        pass

    @abstractmethod
    def removePath(self, path):
        pass
