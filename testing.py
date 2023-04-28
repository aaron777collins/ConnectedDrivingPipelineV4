from Decorators.FileCache import FileCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Logger.Logger import Logger
from Logger.Logger import DEFAULT_LOG_PATH
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IPathProvider import IPathProvider
from ServiceProviders.PathProvider import PathProvider


class Testing:
    @StandardDependencyInjection
    def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider):
        self._pathprovider = pathprovider()
        self._contextprovider = contextprovider()
        self.logger = Logger("testing")
        self.logger.log("Testing")



if __name__ == "__main__":
    pp = PathProvider(model="test",
        contexts={
        "Logger.logpath": DEFAULT_LOG_PATH,
        }
    )
    dp = DictProvider()
    testingclass = Testing()


