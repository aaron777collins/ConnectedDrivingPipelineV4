import os
from pandas import DataFrame, concat
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXY import CleanerWithFilterWithinRangeXY
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXYAndDateRange import CleanerWithFilterWithinRangeXYAndDateRange
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Generator.Cleaners.ExtraCleaningFunctions.CleanWithTimestamps import CleanWithTimestamps
from Helpers.MathHelper import MathHelper

from Logger.Logger import DEFAULT_LOG_PATH, Logger
from Generator.Cleaners.ConnectedDrivingLargeDataPipelineGathererAndCleaner import ConnectedDrivingLargeDataPipelineGathererAndCleaner
from MachineLearning.MClassifierPipeline import MClassifierPipeline
from MachineLearning.MConnectedDrivingDataCleaner import MConnectedDrivingDataCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider

import copy

CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

LOG_NAME = "MClassifierLargePipelineUserWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVHeadingSpeedCols30attackersTrainRandOffset100To200P1xN106y41P2xN105y41With2SeparatePointsd01to30m04y2021"

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Original_Train_Sample_Size", "Total_Original_Test_Sample_Size", "Total_New_Train_Sample_Size", "Total_New_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1", "train_specificity",
               "test_accuracy", "test_precision", "test_recall", "test_f1", "test_specificity"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class MClassifierLargePipelineUserWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVHeadingSpeedCols30attackersTrainRandOffset100To200P1xN106y41P2xN105y41With2SeparatePointsd01to30m04y2021:

    def __init__(self):

        #################  CONFIG FOR ALL PROPERTIES IN THE PIPELINE #################################################

        # used for the logger's path
        self._pathprovider = PathProvider(model=LOG_NAME, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })

        initialGathererModelName = "CreatingConnectedDrivingDataset"
        self.initialGathererModelName = initialGathererModelName # copying so I can use it elsewhere
        numSubsectionRows = 100000

        # Properties:
        # DataGatherer.filepath
        # DataGatherer.subsectionpath
        # DataGatherer.splitfilespath
        # DataGatherer.lines_per_file
        self._initialGathererPathProvider = InitialGathererPathProvider(model=initialGathererModelName, contexts={
            "DataGatherer.filepath": lambda model: "data/data.csv",
            "DataGatherer.subsectionpath": lambda model: f"data/classifierdata/subsection/{model}/subsection{numSubsectionRows}.csv",
            "DataGatherer.splitfilespath": lambda model: f"data/classifierdata/splitfiles/{model}/",
        }
        )

        # Properties:
        #
        # ConnectedDrivingLargeDataCleaner.cleanedfilespath
        # ConnectedDrivingLargeDataCleaner.combinedcleandatapath
        # MConnectedDrivingLargeDataCleaner.dtypes # AUTO_FILLED
        #
        # MAKE SURE TO CHANGE THE MODEL NAME TO THE PROPER NAME (IE A NAME THAT MATCHES IF
        # IT HAS TIMESTAMPS OR NOT, AND IF IT HAS XY COORDS OR NOT, ETC)
        x_pos = -106.0831353
        y_pos = 41.5430216
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-CCDDWithTimestampsAndWithXYCoords-RandSplit80PercentTrain20PercentTest-30attackertrain-2000mdist-x{x_pos_str}y{y_pos_str}-With2SeparatePoints-dd01to30mm04yyyy2021", contexts={
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model:  f"data/classifierdata/splitfiles/cleaned/{model}/",
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: f"data/classifierdata/splitfiles/combinedcleaned/{model}/combinedcleaned",
        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.cleandatapath
        # MDataClassifier.plot_confusion_matrix_path
        #
        self._mlPathProvider = MLPathProvider(model=LOG_NAME, contexts={
            "MConnectedDrivingDataCleaner.cleandatapathtrain": lambda model: f"data/mclassifierdata/cleaned/{model}/train/clean.csv",
            "MConnectedDrivingDataCleaner.cleandatapathtest": lambda model: f"data/mclassifierdata/cleaned/{model}/test/clean.csv",
            "MDataClassifier.plot_confusion_matrix_path": lambda model: f"data/mclassifierdata/results/{model}/",
        }
        )

        # Properties:
        #
        # DataGatherer.numrows
        # ConnectedDrivingCleaner.x_pos
        # ConnectedDrivingCleaner.y_pos
        # ConnectedDrivingLargeDataCleaner.max_dist
        # ConnectedDrivingLargeDataCleaner.cleanFunc
        # ConnectedDrivingLargeDataCleaner.filterFunc
        # ConnectedDrivingAttacker.SEED
        # ConnectedDrivingCleaner.isXYCoords
        # ConnectedDrivingAttacker.attack_ratio
        # ConnectedDrivingCleaner.cleanParams
        #

        # Cleaned columns are added/modified after these columns are used for filtering
        COLUMNS=["metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_accelset_accelYaw","coreData_speed", "coreData_heading", "coreData_position"]


        self.generatorContextProvider = GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numSubsectionRows,
            "DataGatherer.lines_per_file": 1000000,
            "ConnectedDrivingCleaner.x_pos": x_pos,
            "ConnectedDrivingCleaner.y_pos": y_pos,
            "ConnectedDrivingCleaner.columns": COLUMNS,
            "ConnectedDrivingLargeDataCleaner.max_dist": 2000,
            "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": CleanWithTimestamps,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": CleanWithTimestamps.clean_data_with_timestamps,
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass": CleanerWithFilterWithinRangeXYAndDateRange,
            "ConnectedDrivingLargeDataCleaner.filterFunc": CleanerWithFilterWithinRangeXYAndDateRange.within_rangeXY_and_date_range,
            "CleanerWithFilterWithinRangeXYAndDay.startday": 1,
            "CleanerWithFilterWithinRangeXYAndDay.startmonth": 4,
            "CleanerWithFilterWithinRangeXYAndDay.startyear": 2021,
            "CleanerWithFilterWithinRangeXYAndDay.endday": 30,
            "CleanerWithFilterWithinRangeXYAndDay.endmonth": 4,
            "CleanerWithFilterWithinRangeXYAndDay.endyear": 2021,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingCleaner.isXYCoords": True,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.cleanParams": f"clean_data_with_timestamps-within_rangeXY_and_date_range-WithXYCoords-RandSplit80PercentTrain20PercentTest-30attackertrain-2000mdist-x{x_pos_str}y{y_pos_str}-WithSeparatePts-dd01to30mm04yyyy2021", # makes cached data have info on if/if not we use timestamps for uniqueness

        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.columns
        # MClassifierPipeline.classifier_instances # AUTO_FILLED
        #
        self.MLContextProvider = MLContextProvider(contexts={
            "MConnectedDrivingDataCleaner.columns": [
            # "metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            #  "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            #  "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            # "coreData_id",  # "coreData_position_lat", "coreData_position_long",
            # "coreData_secMark", "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            # "month", "day", "year", "hour", "minute", "second", "pm",
            "coreData_elevation",
            # "coreData_accelset_accelYaw",
            "coreData_speed", "coreData_heading",
            "x_pos", "y_pos", "isAttacker"],

            # "MClassifierPipeline.classifier_instances": [...] # AUTO_FILLED
            "MClassifierPipeline.csvWriter": CSVWriter(f"{LOG_NAME}.csv", CSV_COLUMNS),


        }
        )

        ######### END OF CONFIG FOR ALL PROPERTIES IN THE PIPELINE ##################################################

        self.logger = Logger(LOG_NAME)
        self.csvWriter = self.MLContextProvider.get("MClassifierPipeline.csvWriter")

    def write_entire_row(self, dict):
        row = [" "]*len(CSV_COLUMNS)
        # Writing each variable to the row
        for d in dict:
            row[CSV_FORMAT[d]] = dict[d]

        self.csvWriter.addRow(row)

    def run(self):

        mcdldpgac1 = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        # data: DataFrame = mcdldpgac.getAllRows()
        trainData: DataFrame = mcdldpgac1.getAllRows()
        train = trainData

        # splitting into train and test sets
        seed = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED")

        # temp save the generatorContextProvider dict into a variable
        # so we can swap back after we get the test data
        trainGeneratorContextProviderDict = copy.deepcopy(self.generatorContextProvider.getAll())
        trainGeneratorPathProvider = copy.deepcopy(self._generatorPathProvider.getAll())

        # # now getting new dataset from xN105 and y41 (actual coords)
        # # are x=-105.1159611 and y=41.0982327
        self.generatorContextProvider.add("ConnectedDrivingCleaner.x_pos", -105.1159611)
        self.generatorContextProvider.add("ConnectedDrivingCleaner.y_pos", 41.0982327)

        x_pos = -105.1159611
        y_pos = 41.0982327
        # x_pos = -106.0831353
        # y_pos = 41.5430216
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        generatorPathProviderModel=f"{self.initialGathererModelName}-CCDDWithTimestampsAndWithXYCoords-RandSplit80PercentTrain20PercentTest-30attackertest-2000mdist-x{x_pos_str}y{y_pos_str}-LeftOverFromSeparatePts-dd01to30mm04yyyy2021"

        self.generatorContextProvider.add("ConnectedDrivingCleaner.cleanParams", f"clean_data_with_timestamps-within_rangeXY_and_date_range-WithXYCoords-RandSplit80PercentTrain20PercentTest-30attackertest-2000mdist-x{x_pos_str}y{y_pos_str}-LeftOverFromSeparatePts-dd01to30mm04yyyy2021") # makes cached data have info on if/if not we use timestamps for uniqueness)

        self._generatorPathProvider.model = generatorPathProviderModel
        self._mlPathProvider.model = self._mlPathProvider.model + "-2"

        mcdldpgac2 = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        # data: DataFrame = mcdldpgac.getAllRows()
        testData: DataFrame = mcdldpgac2.getAllRows()

        test = testData

        # deep copy test data generatorContextProvider dict
        testGeneratorContextProviderDict = copy.deepcopy(self.generatorContextProvider.getAll())
        testGeneratorPathProvider = copy.deepcopy(self._generatorPathProvider.getAll())

        originalTrainRows = len(train.index)
        originalTestRows = len(test.index)

        # # mixing the train with a random 20% split of the test data
        # test80, test20 = train_test_split(test, test_size=0.2, random_state=seed)

        # # adding the 20% test data to the train data
        # train = concat([train, test20])

        # test = test80 # test is now 80% of the original test data

        # Removed the pt graft so the points are disjoint again

        newTrainRows = len(train.index)
        newTestRows = len(test.index)

        # swapping back to the train generatorContextProvider dict
        self.generatorContextProvider.set(trainGeneratorContextProviderDict)
        self._generatorPathProvider.set(trainGeneratorPathProvider)

        # cleaning/adding attackers to the data
        train = StandardPositionalOffsetAttacker(train, "train").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()

        # swapping back to the test generatorContextProvider dict
        self.generatorContextProvider.set(testGeneratorContextProviderDict)
        self._generatorPathProvider.set(testGeneratorPathProvider)

        test = StandardPositionalOffsetAttacker(test, "test").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()



        # Cleaning it for the malicious data detection
        mdcleaner_train = MConnectedDrivingDataCleaner(train, "train")
        mdcleaner_test = MConnectedDrivingDataCleaner(test, "test")
        m_train = mdcleaner_train.clean_data().get_cleaned_data()
        m_test = mdcleaner_test.clean_data().get_cleaned_data()

        # splitting into the features and the labels
        attacker_col_name = "isAttacker"
        train_X = m_train.drop(columns=[attacker_col_name], axis=1)
        train_Y = m_train[attacker_col_name]
        test_X = m_test.drop(columns=[attacker_col_name], axis=1)
        test_Y = m_test[attacker_col_name]

        # training the classifiers
        mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y)

        mcp.train()
        mcp.test()

        # getting the results
        results = mcp.calc_classifier_results().get_classifier_results()

        # printing the results
        for mclassifier, train_result, result in results:
            mcp.logger.log(mclassifier)
            mcp.logger.log("Train Set Results:")
            mcp.logger.log("Accuracy: ", train_result[0])
            mcp.logger.log("Precision: ", train_result[1])
            mcp.logger.log("Recall: ", train_result[2])
            mcp.logger.log("F1: ", train_result[3])
            mcp.logger.log("Specificity: ", train_result[4])
            mcp.logger.log("Test Set Results:")
            mcp.logger.log("Accuracy: ", result[0])
            mcp.logger.log("Precision: ", result[1])
            mcp.logger.log("Recall: ", result[2])
            mcp.logger.log("F1: ", result[3])
            mcp.logger.log("Specificity: ", result[4])
            # printing the elapsed training and prediction time
            mcp.logger.log("Elapsed Training Time: ",
                           mclassifier.elapsed_train_time)
            mcp.logger.log("Elapsed Prediction Time: ",
                           mclassifier.elapsed_prediction_time)

            mcp.logger.log("Writing to CSV...")

            # writing entire row to csv
            # columns: "Model", "Total_Train_Time",
            #    "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
            #    "train_accuracy", "train_precision", "train_recall", "train_f1",
            #    "test_accuracy", "test_precision", "test_recall", "test_f1"

            csvrowdata = {
                "Model": mclassifier.classifier.__class__.__name__,
                "Total_Train_Time": mclassifier.elapsed_train_time,
                # train and test have the same number of samples
                "Total_Original_Train_Sample_Size": originalTrainRows,
                # train and test have the same number of samples
                "Total_Original_Test_Sample_Size": originalTestRows,
                "Total_New_Train_Sample_Size": newTrainRows,
                "Total_New_Test_Sample_Size": newTestRows,
                "Train_Time_Per_Sample": mclassifier.elapsed_train_time/len(train_X),
                "Prediction_Train_Set_Time_Per_Sample": mclassifier.elapsed_prediction_train_time/len(train_X),
                "Prediction_Test_Set_Time_Per_Sample": mclassifier.elapsed_prediction_time/len(test_X),
                "train_accuracy": train_result[0],
                "train_precision": train_result[1],
                "train_recall": train_result[2],
                "train_f1": train_result[3],
                "train_specificity": train_result[4],
                "test_accuracy": result[0],
                "test_precision": result[1],
                "test_recall": result[2],
                "test_f1": result[3],
                "test_specificity": result[4]}
            self.write_entire_row(csvrowdata)

        # calculating confusion matrices and storing them
        mcp.logger.log("Calculating confusion matrices and storing...")
        mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices()


if __name__ == "__main__":
    mcplu = MClassifierLargePipelineUserWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVHeadingSpeedCols30attackersTrainRandOffset100To200P1xN106y41P2xN105y41With2SeparatePointsd01to30m04y2021()
    mcplu.run()