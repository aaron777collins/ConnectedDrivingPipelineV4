from typing import List
import pandas as pd
import datetime
from os import path
from os import makedirs


class Helper:

    @classmethod
    def printDf(self, df: pd.DataFrame, rows: int):
        print(df.head(rows))

    @classmethod
    def quickDfStat(self, df: pd.DataFrame):
        self.printDf(df, 5)
        print(df.shape)

    @classmethod
    def quickDfArrStat(self, dfs: List[pd.DataFrame]):
        for df in dfs:
            self.printDf(df, 5)
            print(df.shape)

    @classmethod
    def convertToFormattedTime(self, seconds: int) -> str:
        return str(datetime.timedelta(seconds=seconds))

    @classmethod
    def convertToFormattedTime(self, seconds: int) -> str:
        return str(datetime.timedelta(seconds=seconds))

    @classmethod
    def createPath(self, pathArgs):
        makedirs(path.join(pathArgs), exist_ok=True)
