from abc import ABC, abstractmethod

from ServiceProviders.IDictProvider import IDictProvider
class IGeneratorPathProvider(IDictProvider):

    @abstractmethod
    def setModelName(self, model):
        pass

    @abstractmethod
    def getModelName(self):
        pass

    @abstractmethod
    def getPathWithModelName(self, key, defaultValue=None):
        pass

    @abstractmethod
    def getAllPathsWithModelName(self):
        pass

