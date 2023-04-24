import pandas as pd
import os.path as path
import os
from Decorators.CSVCache import CSVCache
from Gatherer.IDataGatherer import IDataGatherer
from Logger.Logger import Logger
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IKeyProvider import IKeyProvider
from ServiceProviders.IPathProvider import IPathProvider


class DataGatherer(IDataGatherer):

        def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider):
            self._pathprovider = pathprovider()
            self._contextprovider = contextprovider()
            self.logger = Logger("DataGatherer", self._pathprovider.__class__)

            self.data = None
            self.numrows = self._contextprovider.get("DataGatherer.numrows")
            self.filepath = self._pathprovider.getPathWithModelName("DataGatherer.filepath")
            self.subsectionpath = self._pathprovider.getPathWithModelName("DataGatherer.subsectionpath")
            self.subsectionname = self._contextprovider.get("DataGatherer.subsectionname")
            self.splitfilespath = self._pathprovider.getPathWithModelName("DataGatherer.splitfilespath")

        def gather_data(self):
            self.data = self._gather_data(full_file_cache_path=self.subsectionpath)
            return self.data

        # caches the results as a file at the specified path
        @CSVCache
        def _gather_data(self, full_file_cache_path="REPLACE_ME"):
            self.logger.log("Didn't find file. Reading from full dataset.")
            self.data = pd.read_csv(self.filepath, nrows=self.numrows)
            return self.data

        # splits the data for easier cleaning
        def split_large_data(self, lines_per_file=100000) -> pd.DataFrame:

            if path.isfile(self.splitfilespath + "split0.csv"):
                self.logger.log("Found split files! Skipping regeneration.")
                return self

            os.makedirs(path.dirname(self.splitfilespath), exist_ok=True)
            # loop until we have all the data
            # create new file for each 1000 lines

            with open(self.filepath, "r") as f:
                header = next(f)  # read the header from the first line of the file
                for i, line in enumerate(f):
                    if i % lines_per_file == 0:
                        self.logger.log(f"Creating new file for line {i}")
                        if i != 0:
                            file.close()
                        file = open(self.splitfilespath + f"split{i}.csv", "w")
                        file.write(header)  # write the header at the beginning of the file
                    file.write(line)
            file.close()

            return self
