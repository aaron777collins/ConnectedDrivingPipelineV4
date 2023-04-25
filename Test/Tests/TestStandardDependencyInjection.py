import os
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Logger.Logger import Logger
from Logger.Logger import DEFAULT_LOG_PATH
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IPathProvider import IPathProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest
import pandas as pd


class TestStandardDependencyInjection(ITest):

    @StandardDependencyInjection
    class TestClass:
        def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider, somekw=None, somekw2=None):
            self._pathprovider: PathProvider = pathprovider()
            self._contextprovider: DictProvider = contextprovider()

            self.somepath = self._pathprovider.getPathWithModelName("TestClass.somepath")

            self.logger = Logger("testing")

            self.somecontext = self._contextprovider.get("TestClass.somecontext")


    def run(self):
        # testing the decorator
        PathProvider(model="test",
                     contexts={
                            "Logger.logpath": DEFAULT_LOG_PATH,
                            "TestClass.somepath": lambda model: os.path.join(PathProvider.DEFAULT_MODEL_NAME, "somepath"),
                     })
        ContextProvider = DictProvider(contexts={
            "TestClass.somecontext": "somecontext"
        })

        # should not fail
        tc = self.TestClass()

        assert(tc.somepath == os.path.join(PathProvider.DEFAULT_MODEL_NAME, "somepath"))
        assert(tc.somecontext == "somecontext")



    def cleanup(self):
        # clear dependencies
        StandardDependencyInjection.cleanup()
