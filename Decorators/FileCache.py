# uses the keyword operator cache_variables to create a long name to store the return of the function in a file
# and read from it if already there
# the function uses the PathProvider but if the key "cache_path" is not found in the context it will use the default
# path of cache/
from ServiceProviders.PathProvider import PathProvider

import os

# decorator to cache the return of a function in a file
# KWARGS:
# cache_variables: list of variables to use as cache variables (default: all the arguments excluding the kwargs)
# cache_file_type: the file type to use for the cache file (default: txt)
# cache_file_reader_function: the function to use to read the file (default: simple read of a txt file)
# cache_file_writer_function: the function to use to write the file (default: simple write of a txt file)
# full_file_cache_path: OVERRIDES the cache path and uses this path instead
# NOTE: the function being decorated must declare the return type in the function declaration
class FileCache:

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, *args, **kwargs):
        self.KW_ARGS_TO_BE_REMOVED = ["cache_variables", "full_file_cache_path", "cache_file_type", "cache_file_reader_function", "cache_file_writer_function"]

        full_path = None

        if not "full_file_cache_path" in kwargs:

            cache_variables = {}
            if "cache_variables" in kwargs:
                cache_variables = kwargs["cache_variables"]
            else:
                # use all the arguments as cache variables
                cache_variables = list(args)

            cache_file_type = "txt"
            # check if "cache_file_type" is in the kwargs
            if "cache_file_type" in kwargs:
                cache_file_type = kwargs["cache_file_type"]

            # check if the file exists with the name of the function and the cache variables

            # create the file name
            file_name = self.fn.__name__
            for cache_variable in cache_variables:
                file_name += "_" + str(cache_variable)

            full_file_name = os.path.join(file_name + "." + cache_file_type)

            cache_path = PathProvider().getPathWithModelName("cache_path", lambda name: os.path.join("cache", name))
            # create the file path
            full_path = os.path.join(cache_path, file_name + "." + cache_file_type)

        else:
            full_path = kwargs["full_file_cache_path"]

        # check if the file exists
        if os.path.exists(full_path):
            # read the file
            # check if "cache_file_reader_function" is in the kwargs
            if "cache_file_reader_function" in kwargs:
                return kwargs["cache_file_reader_function"](full_path, self.fn.__annotations__["return"])
            else:
                return self.readFile(full_path, self.fn.__annotations__["return"])
        else:
            # call function and save the return in the file
            return_value = self.fn(self, *args, **{k: v for k, v in kwargs.items() if k not in self.KW_ARGS_TO_BE_REMOVED})
            # check if "cache_file_writer_function" is in the kwargs
            if "cache_file_writer_function" in kwargs:
                kwargs["cache_file_writer_function"](full_path, return_value)
            else:
                self.writeFile(full_path, return_value)
            return return_value


    # can be overridden for other file types
    def readFile(self, file_name, data_type):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "r") as file:
            return data_type(file.read())

    # can be overridden for other file types
    def writeFile(self, file_path, content):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            file.write(str(content))
