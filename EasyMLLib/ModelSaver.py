import pickle
import os

from sklearn.ensemble import StackingClassifier

from EasyMLLib.helper import Helper

MODEL_FILE_PATH = "Models"

from typing import TypeVar, Generic

T = TypeVar('T')

class ModelSaver(Generic[T]):

    # Saves the model to a pickle file
    # model: The model to save
    # name: the name to save it as
    def saveModel(self, model, name):
        Helper().createPath(MODEL_FILE_PATH)
        with open(os.path.join(MODEL_FILE_PATH, name), "wb") as file:
            pickle.dump(model, file)
            
    def readModel(self, name: str) -> T:
        pathStr = os.path.join(MODEL_FILE_PATH, name)
        if os.path.exists(pathStr):
            print(f"{pathStr} found! Reading data..")
            model: T = None
            print("Getting model")
            with open(pathStr, "rb") as file:
                model = pickle.load(file)
                return model