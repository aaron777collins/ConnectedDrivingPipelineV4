import os
from Decorators.FileCache import FileCache
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestFileCache(ITest):

    @FileCache
    def some_function(self, a, b)-> int:
        return a + b

    def some_other_func(self, a, b)-> int:
        return TestFileCache._some_other_func(a, b, cache_variables=[a, b])

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
        # should be at the path:
        # cache_path = PathProvider.getAllPathsWithModelName("cache_path", lambda name: os.path.join("cache", name))
        # create the file path
        # full_path = os.path.join(cache_path, file_name + "." + cache_file_type)
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_2.txt")))
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_3.txt")))

        # test that FileCache correctly caches the output of a function
        assert(self.some_other_func(1, 2) == 3)

        assert(self.some_other_func(1, 2) == 3)

        # check that the cache gives a different result for different inputs

        assert(self.some_other_func(1, 3) == 4)

        assert(self.some_other_func(1, 2) == 3)

        # assert that the cache correctly stores a file at the cache location
        # i.e. that the cache is actually being used
        # should be at the path:
        # cache_path = PathProvider.getAllPathsWithModelName("cache_path", lambda name: os.path.join("cache", name))
        # create the file path
        # full_path = os.path.join(cache_path, file_name + "." + cache_file_type)
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "_some_other_func_1_2.txt")))
        assert(os.path.exists(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "_some_other_func_1_3.txt")))

    def cleanup(self):
        # remove the cache files
        try:
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_2.txt"))
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "some_function_1_3.txt"))
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "_some_other_func_1_2.txt"))
            os.remove(os.path.join("cache", PathProvider.DEFAULT_MODEL_NAME, "_some_other_func_1_3.txt"))

        except FileNotFoundError:
            pass

        PathProvider.clear()
