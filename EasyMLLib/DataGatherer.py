import os.path as path
from typing import List

import numpy as np
import pandas as pd

DIB_NAME = "DIB1"
DATASETS_PATH = path.join(DIB_NAME, 'dataRaw', 'ID')
SELECTED_DATA_SET_PATH = path.join("Single", "ET")
CONCAT_FILE_PATH = DIB_NAME
CONCAT_FILE_NAME = "concat-data"
CONCAT_FILE_EXT = ".csv"
CONCAT_FULLPATH_WITHOUT_EXT = path.join(CONCAT_FILE_PATH, CONCAT_FILE_NAME)

CLASSIFIERS = [0, 1, 2]

MIN_ID = 1
MAX_ID = 28

class DataGatherer:
        
    def gatherDataFromFile(self, pathStr, concatFunc=None, concatArgs=[]) -> pd.DataFrame:
        if path.exists(pathStr):
            print(f"{pathStr} found! Reading data..")
            return pd.DataFrame(pd.read_csv(pathStr))
        else:
            if concatFunc != None:
                finalDf = concatFunc(*concatArgs)
                finalDf.to_csv(pathStr, index=False)
                print(f"Saved: {pathStr}")
                return finalDf
            else:
                print("File not found and no concatenation function specified!")
                return None