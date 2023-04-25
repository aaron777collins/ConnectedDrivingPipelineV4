import os
import pandas as pd

from Decorators.FileCache import FileCache
# decorator to cache the return of a function in a file
# KWARGS:
# cache_variables: list of variables to use as cache variables (default: all the arguments excluding the kwargs)
# full_file_cache_path: OVERRIDES the cache path and uses this path instead
# NOTE: the function being decorated must declare the return type in the function declaration
class CSVCache:

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, *args, **kwargs):
        # cache_file_type: the file type to use for the cache file (default: txt)
# cache_file_reader_function: the function to use to read the file (default: simple read of a txt file)
# cache_file_writer_function:
        customkwargs = kwargs.copy()
        kwargs["cache_file_type"] = "csv"
        def read_csv(path, return_type):
            # create path if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)
            data = pd.read_csv(path)
            try:
                data.drop("Unnamed: 0",axis=1, inplace=True)
            except:
                pass
            return data
        kwargs["cache_file_reader_function"] = lambda path, return_type: pd.read_csv(path)
        def write_csv(path, data):
            # create path if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)
            data.to_csv(path, index=False)
            try:
                data.drop("Unnamed: 0",axis=1, inplace=True)
            except:
                pass
            return data
        kwargs["cache_file_writer_function"] = lambda path, data: write_csv(path, data)
        return FileCache.__call__(self, *args, **kwargs)

