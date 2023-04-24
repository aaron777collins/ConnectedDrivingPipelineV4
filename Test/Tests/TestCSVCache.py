import os
from Decorators.CSVCache import CSVCache
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest
import pandas as pd


class TestCSVCache(ITest):

    @CSVCache
    def some_function(self, a, b)-> int:
        # create dataframe with a and b in 2 columns, 'a' and 'b'
        df = pd.DataFrame({'a': [a], 'b': [b]})
        return df


    def run(self):
        print(self.some_function(1, 2))
        print(pd.DataFrame({'a': [1], 'b': [2]}))
        print(self.some_function(1, 2).equals(pd.DataFrame({'a': [1], 'b': [2]})))
        # check it works with a dataframe and with cache
        assert(self.some_function(1, 2).equals(pd.DataFrame({'a': [1], 'b': [2]})))
        assert(self.some_function(1, 2).equals(pd.DataFrame({'a': [1], 'b': [2]})))

        # check it works with a different dataframe
        assert(self.some_function(1, 3).equals(pd.DataFrame({'a': [1], 'b': [3]})))
        assert(self.some_function(1, 3).equals(pd.DataFrame({'a': [1], 'b': [3]})))

        # check that the cache gives a different result for different inputs
        assert(self.some_function(1, 2).equals(pd.DataFrame({'a': [1], 'b': [2]})))
        assert(self.some_function(1, 3).equals(pd.DataFrame({'a': [1], 'b': [3]})))

        # check that the cache correctly stores a file at the cache location
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_2.csv")))
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_3.csv")))

    def cleanup(self):
        # remove the cache files
        try:
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_2.csv"))
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_3.csv"))
        except FileNotFoundError:
            pass

        PathProvider.clear()
