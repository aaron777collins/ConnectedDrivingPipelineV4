import os
from Decorators.FileCache import FileCache
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest
import hashlib

class TestFileCache(ITest):

    @FileCache
    def some_function(self, a, b)-> int:
        return a + b

    def some_other_func(self, a, b)-> int:
        return self._some_other_func(a, b, cache_variables=[a, b])

    @FileCache
    def _some_other_func(self, a, b, cache_variables=["REPLACE ME"])-> int:
        return a + b

    def run(self):
        # test that FileCache correctly caches the output of a function
        assert(self.some_function(1, 2) == 3)

        assert(self.some_function(1, 2) == 3)

        # check that the cache gives a different result for different inputs

        assert(self.some_function(1, 3) == 4)

        assert(self.some_function(1, 2) == 3)

        # assert that the cache correctly stores a file at the cache location
        # i.e. that the cache is actually being used
        assert(os.path.exists(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt"))
        assert(os.path.exists(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('some_function_1_3'.encode()).hexdigest()}.txt"))

        # test that FileCache correctly caches the output of a function
        assert(self.some_other_func(1, 2) == 3)

        assert(self.some_other_func(1, 2) == 3)

        # check that the cache gives a different result for different inputs

        assert(self.some_other_func(1, 3) == 4)

        assert(self.some_other_func(1, 2) == 3)

        # assert that the cache correctly stores a file at the cache location
        # i.e. that the cache is actually being used
        assert(os.path.exists(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_some_other_func_1_2'.encode()).hexdigest()}.txt"))
        assert(os.path.exists(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_some_other_func_1_3'.encode()).hexdigest()}.txt"))

    def cleanup(self):
        # remove the cache files
        try:
            os.remove(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt")
            os.remove(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('some_function_1_3'.encode()).hexdigest()}.txt")
            os.remove(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_some_other_func_1_2'.encode()).hexdigest()}.txt")
            os.remove(f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_some_other_func_1_3'.encode()).hexdigest()}.txt")

        except FileNotFoundError:
            pass

        PathProvider.clear()
