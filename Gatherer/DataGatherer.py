import pandas as pd
import os.path as path
import os
from Decorators.CSVCache import CSVCache
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Gatherer.IDataGatherer import IDataGatherer
from Logger.Logger import Logger
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider
from ServiceProviders.IKeyProvider import IKeyProvider
from ServiceProviders.IPathProvider import IPathProvider

# MAKE SURE THAT THE DEPENDENCIES ARE LAST IN THE ARGS
class DataGatherer(IDataGatherer):
        @StandardDependencyInjection
        def __init__(self, pathprovider: IInitialGathererPathProvider, contextprovider: IGeneratorContextProvider):
            self._initialGathererPathProvider = pathprovider()
            self._generatorContextProvider = contextprovider()
            self.logger = Logger("DataGatherer")

            self.data = None
            self.numrows = self._generatorContextProvider.get("DataGatherer.numrows")
            self.filepath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.filepath")
            self.subsectionpath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.subsectionpath")
            self.splitfilespath = self._initialGathererPathProvider.getPathWithModelName("DataGatherer.splitfilespath")

        def gather_data(self):
            self.data = self._gather_data(full_file_cache_path=self.subsectionpath)
            return self.data

        # caches the results as a file at the specified path
        @CSVCache
        def _gather_data(self, full_file_cache_path="REPLACE_ME") -> pd.DataFrame:
            self.logger.log("Didn't find file. Reading from full dataset.")
            self.data = pd.read_csv(self.filepath, nrows=self.numrows)
            return self.data

        # splits the data for easier cleaning
        def split_large_data(self) -> pd.DataFrame:
            lines_per_file = self._generatorContextProvider.get("DataGatherer.lines_per_file")
            os.makedirs(path.dirname(self.splitfilespath), exist_ok=True)

            if path.isfile(f"{self.splitfilespath}split0.csv"):
                self.logger.log("Found split files! Skipping regeneration.")
                return self

            # loop until we have all the data
            # create new file for each 1000 lines

            with open(self.filepath, "r") as f:
                header = next(f)  # read the header from the first line of the file
                for i, line in enumerate(f):
                    if i % lines_per_file == 0:
                        self.logger.log(f"Creating new file for line {i}")
                        if i != 0:
                            file.close()
                        file = open(f"{self.splitfilespath}split{i}.csv", "w")
                        file.write(header)  # write the header at the beginning of the file
                    file.write(line)
            file.close()

            return self

        def get_gathered_data(self):
            return self.data
