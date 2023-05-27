from sklearn.model_selection import train_test_split
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

    while input("Continue? (y/n): ") != "n":

        prefix = input("Prefix: ")

        pp = PathProvider(model=prefix + "test",
            contexts={
            "Logger.logpath": DEFAULT_LOG_PATH,
            }
        )
        dp = DictProvider(contexts={"prefix": prefix})
        testingclass = Testing()
        print(testingclass._contextprovider.get("prefix"))


        train, test = train_test_split([1, 2, 3, 4, 5, 6], test_size=0.2, random_state=75)
        print(train)
        print(test)



