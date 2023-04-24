from Logger.Logger import Logger
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IPathProvider import IPathProvider
from ServiceProviders.PathProvider import PathProvider


class Testing:
    def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider):
        self._pathprovider = pathprovider()
        self._contextprovider = contextprovider()
        self.logger = Logger("testing", self._pathprovider.__class__)
        self.logger.log("Testing")

if __name__ == "__main__":
    pp = PathProvider(model="test",
        contexts={
        "Logger.logpath": Logger.DEFAULT_LOG_PATH,
        }
    )
    dp = DictProvider()
    Testing(PathProvider, DictProvider)
