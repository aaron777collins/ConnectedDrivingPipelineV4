

from Helpers.BColors import BColors
from ServiceProviders.IPathProvider import IPathProvider


class PathProvider(IPathProvider):

    DEFAULT_MODEL_NAME = "NO_MODEL_PROVIDED"

    def __init__(self, contexts=None, model=None):
        self.contexts: dict[str, str] = contexts
        self.model= model
        if contexts is None:
            self.contexts: dict[str, str] = {}
        if model is None:
            self.model: str = PathProvider.DEFAULT_MODEL_NAME
            print(BColors.WARNING + f"WARNING: NO MODEL PROVIDED so the class was set to: {PathProvider.DEFAULT_MODEL_NAME}" + BColors.ENDC)


    def get(self, key):
        return self.contexts[key]

    def set(self, contexts: dict[str,str]):
        self.contexts = contexts

    def add(self, key, context):
        self.contexts[key] = context

    def remove(self, key):
        del self.contexts[key]

    def setModelName(self, model):
        self.model = model

    def getModelName(self):
        return self.model

    def getPathWithModelName(self, key):
        return self.get(key)(self.getModelName())

    def getAll(self):
        return self.contexts

    def getAllPathsWithModelName(self):
        paths = {}
        for key in self.getAll():
            paths[key] = self.getPathWithModelName(key)
        return paths
