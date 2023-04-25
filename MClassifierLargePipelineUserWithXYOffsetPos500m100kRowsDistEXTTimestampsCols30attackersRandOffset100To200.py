# MClassifierPipeline-Const-50-offset

import os
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from EasyMLLib.CSVWriter import CSVWriter

from Logger.Logger import DEFAULT_LOG_PATH, Logger
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider


CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

LOG_NAME = "MClassifierLargePipelineUserWithXYOffsetPos500m100kRowsDistEXTTimestampsCols30attackersRandOffset100To200"

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1",
               "test_accuracy", "test_precision", "test_recall", "test_f1"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class MClassifierLargePipelineUserWithXYOffsetPos500m100kRowsDistEXTTimestampsCols30attackersRandOffset100To200:

    def __init__(self):

        # CONFIG FOR ALL PROPERTIES IN THE PIPELINE

        # TODO: FINISH THIS CLASS

        # used for the logger's path
        self._pathprovider = PathProvider(model=LOG_NAME, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })

        # Properties:
        #
        self._initialGathererPathProvider = InitialGathererPathProvider(model="CreatingConnectedDrivingDataset", contexts={

        }
        )

        # Properties:
        #
        # MAKE SURE TO CHANGE THE MODEL NAME TO THE PROPER NAME (IE A NAME THAT MATCHES IF
        # IT HAS TIMESTAMPS OR NOT, AND IF IT HAS XY COORDS OR NOT, ETC)
        self._generatorPathProvider = GeneratorPathProvider(model="CCDDWithTimestampsAndWithXYCoords", contexts={

        }
        )

        # Properties:
        #
        self._mlPathProvider = MLPathProvider(model=LOG_NAME, contexts={

        }
        )

        # Properties:
        #
        self.generatorContextProvider = GeneratorContextProvider(contexts={

        }
        )

        # Properties:
        #
        self.MLContextProvider = MLContextProvider(contexts={

        }
        )



        self.logger = Logger(LOG_NAME)
        self.csvWriter = CSVWriter(f"{LOG_NAME}.csv", CSV_COLUMNS)

    def write_entire_row(self, dict):
        row = [" "]*len(CSV_COLUMNS)
        # Writing each variable to the row
        for d in dict:
            row[CSV_FORMAT[d]] = dict[d]

        self.csvWriter.addRow(row)

    def run(self):
        ldpgacuLogName = "LargeDataPipelineGathererAndCleanerWithCustomXYUserDist500m"
        ldpgacu = LargeDataPipelineGathererAndCleanerWithCustomXYUserDist(
            logger=Logger(ldpgacuLogName),
            csvWriter=CSVWriter(f"{ldpgacuLogName}.csv", CSV_COLUMNS, outputpath=os.path.join("data", "classifierdata", "results")),
            LOG_NAME=ldpgacuLogName
        ).run(
            cleanFunc=DataCleaner.clean_data_with_OTHER_FUNC_Then_XY,
            otherCleanFunc=DataCleaner.clean_data_with_timestamps,
            filterFunction=LargeDataCleaner.within_rangeXY,
            max_dist=500, x_col="x_pos", y_col="y_pos",
            x_pos=-105.1159611, y_pos=41.0982327, lines_per_file=1000000
        )

        data: DataFrame = ldpgacu.getNRows(200000)

        # splitting into train and test sets
        train = data.iloc[:100000].copy()
        test = data.iloc[100000:200000].copy()

        # cleaning/adding attackers to the data
        train = DataAttacker(train,
                             modified_data_path=f"data/classifierdata/modified/{LOG_NAME}/modified_train.csv", SEED=24, logger=self.logger.newPrefix("DataAttacker"),
                             isXYCoords=True).add_attackers(attack_ratio=0.3).add_attacks_positional_offset_rand(min_dist=50000, max_dist=100000).getData()
        test = DataAttacker(test,
                            modified_data_path=f"data/classifierdata/modified/{LOG_NAME}/modified_test.csv", SEED=48, logger=self.logger.newPrefix("DataAttacker"),
                            isXYCoords=True).add_attackers(attack_ratio=0.3).add_attacks_positional_offset_rand(min_dist=50000, max_dist=100000).getData()

        # Normally ["coreData_id", "coreData_position_lat", "coreData_position_long",
        # "coreData_elevation", "coreData_accelset_accelYaw","coreData_speed", "coreData_heading", "x_pos", "y_pos", "isAttacker"]

        COLUMNS_EXT_WITH_TIMESTAMPS = [
            # "metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            #  "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            #  "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            "coreData_id",  # "coreData_position_lat", "coreData_position_long",
            "coreData_secMark", "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "month", "day", "year", "hour", "minute", "second", "pm",
            "coreData_elevation", "coreData_accelset_accelYaw", "coreData_speed", "coreData_heading", "x_pos", "y_pos", "isAttacker"]

        # Cleaning it for the malicious data detection
        mdcleaner_train = MDataCleaner(
            train, cleandatapath=f"data/classifierdata/Mclean/{LOG_NAME}/clean_train.csv", columns=COLUMNS_EXT_WITH_TIMESTAMPS, logger=self.logger.newPrefix("MDataCleaner"))
        mdcleaner_test = MDataCleaner(
            test, cleandatapath=f"data/classifierdata/Mclean/{LOG_NAME}/clean_test.csv", columns=COLUMNS_EXT_WITH_TIMESTAMPS, logger=self.logger.newPrefix("MDataCleaner"))
        m_train = mdcleaner_train.clean_data().get_cleaned_data()
        m_test = mdcleaner_test.clean_data().get_cleaned_data()

        # splitting into X and Y
        attacker_col_name = "isAttacker"
        train_X = m_train.drop(columns=[attacker_col_name], axis=1)
        train_Y = m_train[attacker_col_name]
        test_X = m_test.drop(columns=[attacker_col_name], axis=1)
        test_Y = m_test[attacker_col_name]

        # training the classifiers
        mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y, classifier_instances=CLASSIFIER_INSTANCES,
                                  logger=self.logger.newPrefix("MClassifierPipeline"))

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
            mcp.logger.log("Test Set Results:")
            mcp.logger.log("Accuracy: ", result[0])
            mcp.logger.log("Precision: ", result[1])
            mcp.logger.log("Recall: ", result[2])
            mcp.logger.log("F1: ", result[3])
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
                "Total_Train_Sample_Size": len(train_X),
                # train and test have the same number of samples
                "Total_Test_Sample_Size": len(test_X),
                "Train_Time_Per_Sample": mclassifier.elapsed_train_time/len(train_X),
                "Prediction_Train_Set_Time_Per_Sample": mclassifier.elapsed_prediction_train_time/len(train_X),
                "Prediction_Test_Set_Time_Per_Sample": mclassifier.elapsed_prediction_time/len(test_X),
                "train_accuracy": train_result[0],
                "train_precision": train_result[1],
                "train_recall": train_result[2],
                "train_f1": train_result[3],
                "test_accuracy": result[0],
                "test_precision": result[1],
                "test_recall": result[2],
                "test_f1": result[3]}
            self.write_entire_row(csvrowdata)

        # calculating confusion matrices and storing them
        mcp.logger.log("Calculating confusion matrices and storing...")
        # path to store the confusion matrices
        plotpath = f"data/classifierdata/results/plots/{LOG_NAME}/confusion_matrices"
        mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices(plotpath)


if __name__ == "__main__":
    mcplu = MClassifierLargePipelineUserWithXYOffsetPos500m100kRowsDistEXTTimestampsCols30attackersRandOffset100To200()
    mcplu.run()
