import os
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from Test.ITest import ITest
import pandas as pd


class TestChildContextProviders(ITest):

    class SomeClass:
        @StandardDependencyInjection
        def __init__(self, generatorContextProvider: IGeneratorContextProvider, mlContextProvider: IMLContextProvider):
            self._generatorContextProvider = generatorContextProvider()
            self._mlContextProvider = mlContextProvider()

            self.somevar = self._generatorContextProvider.get("a")
            self.somevar2 = self._mlContextProvider.get("a")



    def run(self):
        gcp = GeneratorContextProvider()
        gcp.add("a", "b")

        mlcp = MLContextProvider()
        mlcp.add("a", "d")

        sc = self.SomeClass()

        assert(sc.somevar == "b")
        assert(sc.somevar2 == "d")

        assert(gcp.getAll() == {"a": "b"})
        assert(mlcp.getAll() == {"a": "d"})

    def cleanup(self):
        GeneratorContextProvider.clear()
        MLContextProvider.clear()
