import os.path as path
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

TEST_SPLIT_PERCENT = 0.4
VAL_TEST_SPLIT_PERCENT = 0.5
SEED = 42
CLASSIFIER_NAME='classifier'

class DataSplitter:
    def __init__(self, testSplitPercent=0.4, valTestSplitPercent=0.5, seed=42, classifierName='classifier'):
        TEST_SPLIT_PERCENT = testSplitPercent
        VAL_TEST_SPLIT_PERCENT = valTestSplitPercent
        SEED=seed
        self.classifierName=classifierName


    # returns [features, answers] (assumes classifier is a column (or the name is set to the classifier name))
    def getAllFeaturesAndAnswers(self, data:pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return [data.drop([self.classifierName], axis=1), data[self.classifierName]]

    #returns [X_train, Y_train, X_val, Y_val, X_test, Y_test]
    def getTrainValTestSplit(self, data:pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,pd.DataFrame, pd.DataFrame]:
        features, answers = self.getAllFeaturesAndAnswers(data)

        X_train, X_test, Y_train, Y_test = train_test_split(features, answers, test_size=TEST_SPLIT_PERCENT, random_state=SEED)
        X_val, X_test, Y_val, Y_test = train_test_split(X_test, Y_test, test_size=VAL_TEST_SPLIT_PERCENT, random_state=SEED)
        return (X_train, Y_train, X_val, Y_val, X_test, Y_test)
