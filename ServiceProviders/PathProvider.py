from ServiceProviders.IPathProvider import IPathProvider


class PathProvider(IPathProvider):
    def __init__(self, paths=None):
        self.paths: dict[str, str] = paths
        if paths is None:
            self.paths: dict[str, str] = {}

    def getPaths(self):
        return self.paths

    def setPaths(self, paths: dict[str,str]):
        self.paths = paths

    def addPath(self, key, path):
        self.paths[key] = path

    def removePath(self, key):
        del self.paths[key]
