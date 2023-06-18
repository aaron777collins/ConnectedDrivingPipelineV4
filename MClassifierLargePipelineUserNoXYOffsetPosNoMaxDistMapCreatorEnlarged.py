import os
from matplotlib import pyplot as plt
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXY import CleanerWithFilterWithinRangeXY
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXYAndDateRange import CleanerWithFilterWithinRangeXYAndDateRange
from Generator.Cleaners.CleanersWithFilters.CleanerWithPassthroughFilter import CleanerWithPassthroughFilter
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Generator.Cleaners.ExtraCleaningFunctions.CleanWithTimestamps import CleanWithTimestamps
from Helpers.ImageWriter import ImageWriter
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


CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

LOG_NAME = "MClassifierLargePipelineUserNoXYOffsetPosNoMaxDistMapCreatorEnlarged"

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1",
               "test_accuracy", "test_precision", "test_recall", "test_f1"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class MClassifierLargePipelineUserNoXYOffsetPosNoMaxDistMapCreatorEnlarged:

    def __init__(self):

        #################  CONFIG FOR ALL PROPERTIES IN THE PIPELINE #################################################

        # used for the logger's path
        self._pathprovider = PathProvider(model=LOG_NAME, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })

        initialGathererModelName = "CreatingConnectedDrivingDataset"
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
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-CCDDWithTimestampsAndWithoutXYCoords-RandSplit80ktrain-20ktest-2000mdist-x{x_pos_str}y{y_pos_str}-dd01to30mm04yyyy2021", contexts={
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
            "MDataClassifier.plot_distribution_path": lambda model: f"data/mclassifierdata/results/{model}/",
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
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass": CleanerWithPassthroughFilter,
            "ConnectedDrivingLargeDataCleaner.filterFunc": CleanerWithPassthroughFilter.passthrough,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingCleaner.isXYCoords": False,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.cleanParams": f"clean_data_with_timestamps-within_rangeXY_and_date_range-WithoutXYCoords-RandSplit80ktrain-20ktest-2000mdist-x{x_pos_str}y{y_pos_str}dd01to30mm04yyyy2021", # makes cached data have info on if/if not we use timestamps for uniqueness

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
            "coreData_id",  # "coreData_position_lat", "coreData_position_long",
            "coreData_secMark", "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "month", "day", "year", "hour", "minute", "second", "pm",
            "coreData_elevation", "coreData_accelset_accelYaw", "coreData_speed", "coreData_heading", "x_pos", "y_pos", "isAttacker"],

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

        mcdldpgac = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        data: DataFrame = mcdldpgac.getAllRows()

        # add headers to csv
        headerRow = [" "]*len(CSV_COLUMNS)
        headerRow[0] = "Title"
        headerRow[1] = "NumRows"
        headerRow[2] = "Image"

        self.csvWriter.addRow(headerRow)

        self.plot_data(data, "x_pos", "y_pos", f"x_pos vs y_pos for all rows")


    def plot_data(self, data: DataFrame, x: str, y: str, title: str):
        # reset plot
        plt.clf()
        plt.figure(figsize=(100, 80))
        # xlim as min and max of x
        plt.xlim(data[x].min(), data[x].max())
        # ylim as min and max of y
        plt.ylim(data[y].min(), data[y].max())
        plt.scatter(data[x], data[y])
        plt.title(title)
        plt.xlabel(x)
        plt.ylabel(y)
        # save to the plots folder
        plotPath = self._mlPathProvider.getPathWithModelName("MDataClassifier.plot_distribution_path")

        finalPlotPath = plotPath + f"{title}.png"

        os.makedirs(plotPath, exist_ok=True)

        plt.savefig(finalPlotPath)

        # write image to the csv
        imageWriter = ImageWriter(self.csvWriter)
        imageWriter.writeImageAtEndOfRow([title,  len(data)], imageWriter.readImageAsBase64Array(finalPlotPath))


if __name__ == "__main__":
    mcplu = MClassifierLargePipelineUserNoXYOffsetPosNoMaxDistMapCreatorEnlarged()
    mcplu.run()
