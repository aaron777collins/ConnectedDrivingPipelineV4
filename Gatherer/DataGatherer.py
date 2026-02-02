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
            self.logger.log("Error tolerance enabled: on_bad_lines='skip' (malformed rows will be skipped)")
            
            # Read CSV with error tolerance - skip malformed rows
            self.data = pd.read_csv(
                self.filepath,
                nrows=self.numrows,
                on_bad_lines='skip'  # Skip rows with parsing errors (e.g., EOF inside string)
            )
            
            # Log loaded rows and estimate skipped lines
            loaded_rows = len(self.data)
            self.logger.log(f"Loaded {loaded_rows:,} rows from CSV")
            
            try:
                import subprocess
                result = subprocess.run(
                    ['wc', '-l', self.filepath],
                    capture_output=True, text=True, timeout=300
                )
                if result.returncode == 0:
                    source_lines = int(result.stdout.strip().split()[0])
                    expected_data_rows = source_lines - 1  # Subtract header
                    # Account for nrows limit
                    if self.numrows and self.numrows > 0:
                        expected_data_rows = min(expected_data_rows, self.numrows)
                    skipped_rows = expected_data_rows - loaded_rows
                    if skipped_rows > 0:
                        self.logger.log(f"⚠️  SKIPPED {skipped_rows:,} malformed rows")
                    else:
                        self.logger.log(f"✓ All rows loaded successfully (no rows skipped)")
            except Exception as e:
                self.logger.log(f"Could not count skipped rows: {e}")
            
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
