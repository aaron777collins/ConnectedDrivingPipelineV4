from datetime import datetime
import os.path as path
import string
from typing import Tuple

import numpy as np
import pandas as pd

from EasyMLLib.helper import Helper

OUTPUT_FOLDER_PATH = path.join("Outputs", "Output")

class Logger:
    def __init__(self, name: string):
        self.name = name
        Helper().createPath(OUTPUT_FOLDER_PATH)
        timeLog = datetime.now().strftime(r'%Y-%m-%d_%H-%M-%S')
        self.log("### "+ timeLog + " ###")

        
        
    def log(self, *args):
        with open(path.join(OUTPUT_FOLDER_PATH, self.name), "a+") as file:
            for elem in args:
                print(str(elem) + " ", end='')
                file.write(str(elem) + " ")
            file.write("\n")
            print()