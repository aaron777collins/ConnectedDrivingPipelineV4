from csv import writer
import os.path as path
import string
from typing import List

from EasyMLLib.helper import Helper

OUTPUT_FOLDER_PATH = path.join("Outputs", "Output")

class CSVWriter:
    def __init__(self, name: string, columns: List[str], outputpath=OUTPUT_FOLDER_PATH):
        self.name = name
        self.columns = columns
        self.outputpath = outputpath
        Helper().createPath(outputpath)
        with open(path.join(outputpath, self.name), "a+", newline='') as file:
                writerObj = writer(file)
                writerObj.writerow(columns)


    def addRow(self, row: list):
        with open(path.join(self.outputpath, self.name), "a+", newline='') as file:
            writerObj = writer(file)
            writerObj.writerow(row)
