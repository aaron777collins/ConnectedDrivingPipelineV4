"""
Dask-based MClassifier Pipeline for ConstOffsetPerID 100-200m attack detection.

EXTENDED FEATURES version - includes more BSM features for ML:
- x_pos, y_pos (position)
- coreData_elevation
- coreData_speed
- coreData_accelset_accelYaw
- coreData_heading
- coreData_accuracy_semiMajor

Parameters:
- Distance: 2000m from origin point (-106.0831353, 41.5430216)
- Date Range: April 1-30, 2021 (full month)
- Attack: Constant Offset Per ID 100-200m (random direction per vehicle)
- Attack ratio: 30%
- Train/Test: 80/20 random split

Author: Sophie (AI Research Assistant)
Supervisor: Aaron Collins
Date: 2026-02-12
"""

import hashlib
import os
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

# Dask imports
import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
from Generator.Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps
from Helpers.MathHelper import MathHelper

from Logger.Logger import DEFAULT_LOG_PATH, Logger
from Generator.Cleaners.DaskConnectedDrivingLargeDataPipelineGathererAndCleaner import DaskConnectedDrivingLargeDataPipelineGathererAndCleaner
from MachineLearning.MClassifierPipeline import MClassifierPipeline
from MachineLearning.MConnectedDrivingDataCleaner import MConnectedDrivingDataCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider


CLASSIFIER_INSTANCES = [RandomForestClassifier(), DecisionTreeClassifier(), KNeighborsClassifier()]

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1", "train_specificity",
               "test_accuracy", "test_precision", "test_recall", "test_f1", "test_specificity"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class DaskMClassifierConstOffsetPerIDExtFeatures100To200:
    """
    Dask-based pipeline for ML classification with EXTENDED FEATURES.
    
    Uses additional BSM features beyond just position:
    - Speed, heading, acceleration, GPS accuracy
    
    Attack: Constant positional offset per vehicle ID (100-200m)
    """
    
    @classmethod
    def getClassNameHash(cls):
        return hashlib.md5(cls.__name__.encode()).hexdigest()

    def __init__(self):
        self._pathprovider = PathProvider(model=self.__class__.__name__, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })
        
        self.logger = Logger(self.__class__.__name__)
        
        # Initialize Dask client
        dask_config = os.environ.get('DASK_CONFIG')
        if dask_config:
            self.logger.log(f"Loading Dask config from: {dask_config}")
            dask.config.refresh()
        
        self.logger.log("Initializing Dask LocalCluster...")
        try:
            self.cluster = LocalCluster(
                n_workers=2,
                threads_per_worker=2,
                memory_limit='28GB',
                silence_logs=30,
            )
            self.client = Client(self.cluster)
            self.logger.log(f"Dask dashboard available at: {self.client.dashboard_link}")
        except Exception as e:
            self.logger.log(f"Warning: Could not start Dask cluster: {e}")
            self.client = None

        #################  CONFIG  #################################################

        initialGathererModelName = f"{self.__class__.getClassNameHash()}-CreatingConnectedDrivingDataset"
        numSubsectionRows = -1

        self._initialGathererPathProvider = InitialGathererPathProvider(model=initialGathererModelName, contexts={
            "DataGatherer.filepath": lambda model: "data/data.csv",
            "DataGatherer.subsectionpath": lambda model: f"data/classifierdata/subsection/{model}/subsection{numSubsectionRows}.csv",
            "DataGatherer.splitfilespath": lambda model: f"data/classifierdata/splitfiles/{model}/",
        })

        x_pos = -106.0831353
        y_pos = 41.5430216
        
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-GENERATOR_PATH", contexts={
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: f"data/classifierdata/splitfiles/cleaned/{model}/",
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: f"data/classifierdata/splitfiles/combinedcleaned/{model}/combinedcleaned",
        })

        self._mlPathProvider = MLPathProvider(model=self.__class__.__name__, contexts={
            "MConnectedDrivingDataCleaner.cleandatapathtrain": lambda model: f"data/mclassifierdata/cleaned/{model}/train/clean.csv",
            "MConnectedDrivingDataCleaner.cleandatapathtest": lambda model: f"data/mclassifierdata/cleaned/{model}/test/clean.csv",
            "MDataClassifier.plot_confusion_matrix_path": lambda model: f"data/mclassifierdata/results/{model}/",
        })

        # EXTENDED COLUMNS - includes more features than basic ONLYXYELEV
        COLUMNS = [
            "metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            "metadata_serialId_serialNumber", "metadata_receivedAt",
            "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_accelset_accelYaw", "coreData_speed", "coreData_heading", "coreData_position"
        ]

        self.generatorContextProvider = GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numSubsectionRows,
            "DataGatherer.lines_per_file": 1000000,
            "ConnectedDrivingCleaner.x_pos": x_pos,
            "ConnectedDrivingCleaner.y_pos": y_pos,
            "ConnectedDrivingCleaner.columns": COLUMNS,
            "ConnectedDrivingLargeDataCleaner.max_dist": 2000,
            "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": DaskCleanWithTimestamps,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": DaskCleanWithTimestamps.clean_data_with_timestamps,
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass": DaskCleanerWithFilterWithinRangeXYAndDateRange,
            "ConnectedDrivingLargeDataCleaner.filterFunc": DaskCleanerWithFilterWithinRangeXYAndDateRange.within_rangeXY_and_date_range,
            "CleanerWithFilterWithinRangeXYAndDay.startday": 1,
            "CleanerWithFilterWithinRangeXYAndDay.startmonth": 4,
            "CleanerWithFilterWithinRangeXYAndDay.startyear": 2021,
            "CleanerWithFilterWithinRangeXYAndDay.endday": 30,
            "CleanerWithFilterWithinRangeXYAndDay.endmonth": 4,
            "CleanerWithFilterWithinRangeXYAndDay.endyear": 2021,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingCleaner.isXYCoords": True,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.cleanParams": f"{self.__class__.getClassNameHash()}-CLEAN_PARAMS-DASK",
        })

        # EXTENDED ML COLUMNS - includes speed, heading, acceleration, accuracy
        self.MLContextProvider = MLContextProvider(contexts={
            "MConnectedDrivingDataCleaner.columns": [
                "x_pos", "y_pos",
                "coreData_elevation",
                "coreData_speed",
                "coreData_accelset_accelYaw",
                "coreData_heading",
                "coreData_accuracy_semiMajor",
                "isAttacker"
            ],
            "MClassifierPipeline.csvWriter": CSVWriter(f"{self.__class__.__name__}.csv", CSV_COLUMNS),
        })

        self.csvWriter = self.MLContextProvider.get("MClassifierPipeline.csvWriter")

    def write_entire_row(self, dict):
        row = [" "]*len(CSV_COLUMNS)
        for d in dict:
            row[CSV_FORMAT[d]] = dict[d]
        self.csvWriter.addRow(row)

    def run(self):
        self.logger.log("=" * 60)
        self.logger.log("DASK PIPELINE: ConstOffsetPerID 100-200m (EXTENDED FEATURES)")
        self.logger.log("=" * 60)
        self.logger.log("Features: x_pos, y_pos, elevation, speed, accelYaw, heading, accuracy")
        
        if self.client:
            self.logger.log(f"Workers: {len(self.client.scheduler_info()['workers'])}")
            total_mem = sum(w['memory_limit'] for w in self.client.scheduler_info()['workers'].values())
            self.logger.log(f"Total memory: {total_mem / 1e9:.1f} GB")
        
        self.logger.log("Starting data gathering and cleaning...")
        mcdldpgac = DaskConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        self.logger.log("Loading all cleaned rows into memory...")
        data: DataFrame = mcdldpgac.getAllRows()
        
        total_rows = len(data)
        self.logger.log(f"Total cleaned rows: {total_rows:,}")

        # 80/20 random split
        seed = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED")
        numRowsToTrain = int(total_rows * 0.8)

        self.logger.log(f"Splitting data: {numRowsToTrain:,} train, {total_rows - numRowsToTrain:,} test")
        train = data.head(numRowsToTrain)
        test = data.tail(len(data)-numRowsToTrain)

        # Add attacks - constant offset per vehicle ID with random direction
        self.logger.log("Adding ConstOffsetPerID 100-200m attacks to train set...")
        train = StandardPositionalOffsetAttacker(train, "train").add_attackers().add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=100, max_dist=200).get_data()
        
        self.logger.log("Adding ConstOffsetPerID 100-200m attacks to test set...")
        test = StandardPositionalOffsetAttacker(test, "test").add_attackers().add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=100, max_dist=200).get_data()

        # Clean for ML
        self.logger.log("Cleaning data for ML pipeline...")
        mdcleaner_train = MConnectedDrivingDataCleaner(train, "train")
        mdcleaner_test = MConnectedDrivingDataCleaner(test, "test")
        m_train = mdcleaner_train.clean_data().get_cleaned_data()
        m_test = mdcleaner_test.clean_data().get_cleaned_data()

        # Split features/labels
        attacker_col_name = "isAttacker"
        train_X = m_train.drop(columns=[attacker_col_name])
        train_Y = m_train[attacker_col_name]
        test_X = m_test.drop(columns=[attacker_col_name])
        test_Y = m_test[attacker_col_name]

        self.logger.log(f"Training features shape: {train_X.shape}")
        self.logger.log(f"Test features shape: {test_X.shape}")
        self.logger.log(f"Feature columns: {list(train_X.columns)}")

        # Train classifiers
        self.logger.log("=" * 60)
        self.logger.log("TRAINING CLASSIFIERS")
        self.logger.log("=" * 60)
        
        mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y)
        mcp.train()
        mcp.test()

        # Get results
        results = mcp.calc_classifier_results().get_classifier_results()

        # Print results
        for mclassifier, train_result, result in results:
            mcp.logger.log("=" * 40)
            mcp.logger.log(f"CLASSIFIER: {mclassifier.classifier.__class__.__name__}")
            mcp.logger.log("=" * 40)
            mcp.logger.log("Train Set Results:")
            mcp.logger.log("  Accuracy: ", train_result[0])
            mcp.logger.log("  Precision: ", train_result[1])
            mcp.logger.log("  Recall: ", train_result[2])
            mcp.logger.log("  F1: ", train_result[3])
            mcp.logger.log("  Specificity: ", train_result[4])
            mcp.logger.log("Test Set Results:")
            mcp.logger.log("  Accuracy: ", result[0])
            mcp.logger.log("  Precision: ", result[1])
            mcp.logger.log("  Recall: ", result[2])
            mcp.logger.log("  F1: ", result[3])
            mcp.logger.log("  Specificity: ", result[4])

            csvrowdata = {
                "Model": mclassifier.classifier.__class__.__name__,
                "Total_Train_Time": mclassifier.elapsed_train_time,
                "Total_Train_Sample_Size": len(train_X),
                "Total_Test_Sample_Size": len(test_X),
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

        # Confusion matrices
        self.logger.log("Calculating confusion matrices...")
        mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices()
        
        # Cleanup
        if self.client:
            self.logger.log("Shutting down Dask cluster...")
            self.client.close()
            self.cluster.close()
        
        self.logger.log("=" * 60)
        self.logger.log("PIPELINE COMPLETE")
        self.logger.log("=" * 60)


if __name__ == "__main__":
    mcplu = DaskMClassifierConstOffsetPerIDExtFeatures100To200()
    mcplu.run()
