from abc import ABC, abstractmethod

from ServiceProviders.IDictProvider import IDictProvider

class IKeyProvider(IDictProvider):

    @abstractmethod
    def get(self, key):
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
