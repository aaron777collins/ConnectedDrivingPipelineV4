# MClassifierPipeline-Const-50-offset

import os
from matplotlib import pyplot as plt
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXY import CleanerWithFilterWithinRangeXY
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXYAndDay import CleanerWithFilterWithinRangeXYAndDay
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

LOG_NAME = "MClassifierLargePipelineUserWithXYOffsetPos1000mDist1Day1000kRowsEXTTimestampsColsFeatureAnalysisOnDataPointsInRange3Points"

CSV_COLUMNS = ["Model", "rowCount", "Date", "Image"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class MClassifierLargePipelineUserWithXYOffsetPos1000mDist1Day1000kRowsEXTTimestampsColsFeatureAnalysisOnDataPointsInRange3Points:

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
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-CCDDWithTimestampsAndWithXYCoords-1000mdist-3Points-dd02mm04yyyy2021", contexts={
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
            "MConnectedDrivingDataCleaner.cleandatapath": lambda model: f"data/mclassifierdata/cleaned/{model}/clean.csv",
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

        x_pos = -105.1159611
        y_pos = 41.0982327
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        self.generatorContextProvider = GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numSubsectionRows,
            "DataGatherer.lines_per_file": 1000000,
            "ConnectedDrivingCleaner.x_pos": x_pos,
            "ConnectedDrivingCleaner.y_pos": y_pos,
            "ConnectedDrivingCleaner.columns": COLUMNS,
            "ConnectedDrivingLargeDataCleaner.max_dist": 1000,
            "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": CleanWithTimestamps,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": CleanWithTimestamps.clean_data_with_timestamps,
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass": CleanerWithFilterWithinRangeXYAndDay,
            "ConnectedDrivingLargeDataCleaner.filterFunc": CleanerWithFilterWithinRangeXYAndDay.within_rangeXY_and_day,
            "CleanerWithFilterWithinRangeXYAndDay.day": 2,
            "CleanerWithFilterWithinRangeXYAndDay.month": 4,
            "CleanerWithFilterWithinRangeXYAndDay.year": 2021,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingCleaner.isXYCoords": True,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.cleanParams": f"clean_data_with_timestamps-within_rangeXY_and_day-WithXYCoords-1000mdist-x{x_pos_str}y{y_pos_str}dd02mm04yyyy2021", # makes cached data have info on if/if not we use timestamps for uniqueness

        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.columns
        # MClassifierPipeline.classifier_instances # AUTO_FILLED
        # MClassifierPipeline.csvWriter
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

        # default:
        # "ConnectedDrivingCleaner.x_pos": -105.1159611,
        # "ConnectedDrivingCleaner.y_pos": 41.0982327,
        # "CleanerWithFilterWithinRangeXYAndDay.day": 2
        # "CleanerWithFilterWithinRangeXYAndDay.month": 4
        # "CleanerWithFilterWithinRangeXYAndDay.year": 2021
        # "ConnectedDrivingLargeDataCleaner.max_dist": 1000"



        originalGeneratorPathProviderModelName = self._generatorPathProvider.getModelName()
        x_pos = self.generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        y_pos = self.generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")
        day = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.day")
        month = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.month")
        year = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.year")

        # creating new model name with 'feature-analysis', x, y, day, month, year
        self._generatorPathProvider.setModelName(f"{originalGeneratorPathProviderModelName}-feature-analysis-{x_pos}-{y_pos}-{day}-{month}-{year}")


        self.runIteration()

        # setting new x and y positions
        x_pos = -106.0831353
        y_pos = 41.5430216
        print(f"Setting new x and y positions: {x_pos}, {y_pos}")
        self.generatorContextProvider.add(key="ConnectedDrivingCleaner.x_pos", context=x_pos)
        self.generatorContextProvider.add(key="ConnectedDrivingCleaner.y_pos", context=y_pos)
        self._generatorPathProvider.setModelName(f"{originalGeneratorPathProviderModelName}-feature-analysis-f{x_pos}-f{y_pos}-{day}-{month}-{year}")
        # important that we change the cleanParams to avoid cache overlap
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        self.generatorContextProvider.add(key="ConnectedDrivingLargeDataCleaner.cleanParams", context=f"clean_data_with_timestamps-within_rangeXY_and_day-WithXYCoords-1000mdist-x{x_pos_str}y{y_pos_str}dd02mm04yyyy2021")

        self.runIteration()

        # setting new x and y positions
        x_pos = -104.6724598
        y_pos = 41.1518897
        print(f"Setting new x and y positions: {x_pos}, {y_pos}")
        self.generatorContextProvider.add(key="ConnectedDrivingCleaner.x_pos", context=x_pos)
        self.generatorContextProvider.add(key="ConnectedDrivingCleaner.y_pos", context=y_pos)
        self._generatorPathProvider.setModelName(f"{originalGeneratorPathProviderModelName}-feature-analysis-f{x_pos}-f{y_pos}-{day}-{month}-{year}")
        # important that we change the cleanParams to avoid cache overlap
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        self.generatorContextProvider.add(key="ConnectedDrivingLargeDataCleaner.cleanParams", context=f"clean_data_with_timestamps-within_rangeXY_and_day-WithXYCoords-1000mdist-x{x_pos_str}y{y_pos_str}dd02mm04yyyy2021")

        self.runIteration()



    def runIteration(self):

        x_pos = self.generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        y_pos = self.generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

        self.logger.log(f"Running feature analysis with points: {x_pos}, {y_pos}")
        mcdldpgac = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        # Aim to get 1 million rows
        # If we can't get 1 million rows, then we get as many as we can
        data: DataFrame = mcdldpgac.getNRows(1000000)

        # log number of rows
        self.logger.log(f"Number of rows: {len(data)}")

        # plot the data on a chart to see the x and y position distribution
        self.plot_data(data, "x_pos", "y_pos", f"x_pos vs y_pos with x={x_pos} and y={y_pos}")


    def plot_data(self, data: DataFrame, x: str, y: str, title: str):
        # reset plot
        plt.clf()
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
        # date = mm/dd/yyyy from the settings
        day = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.day")
        month = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.month")
        year = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.year")
        # date has filler 0s if needed ex. 1 -> 01
        daystr = str(day) if day >= 10 else f"0{day}"
        monthstr = str(month) if month >= 10 else f"0{month}"
        yearstr = str(year)
        date = f"{monthstr}/{daystr}/{yearstr}"
        imageWriter.writeImageAtEndOfRow([title,  len(data), date], imageWriter.readImageAsBase64Array(finalPlotPath))



if __name__ == "__main__":
    mcplu = MClassifierLargePipelineUserWithXYOffsetPos1000mDist1Day1000kRowsEXTTimestampsColsFeatureAnalysisOnDataPointsInRange3Points()
    mcplu.run()
