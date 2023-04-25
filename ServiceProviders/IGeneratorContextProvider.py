from abc import ABC, abstractmethod

from ClassTypes.SingletonABCMeta import SingletonABCMeta
from ServiceProviders.IDictProvider import IDictProvider

class IGeneratorContextProvider(IDictProvider):

    @abstractmethod
    def get(self, key, defaultValue=None):
        pass

    @abstractmethod
    def set(self, context):
        pass

    @abstractmethod
    def add(self, context):
        pass

    @abstractmethod
    def remove(self, context):
        pass

    @abstractmethod
    def getAll(self):
        pass
