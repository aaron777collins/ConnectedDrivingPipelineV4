"""
Dask-based MClassifier Pipeline for ConstPosPerCar 100-200m attack detection.

This pipeline uses Dask components for distributed data processing:
- DaskCleanerWithFilterWithinRangeXYAndDateRange for filtering
- Parallelized data cleaning across workers
- Respects DASK_CONFIG environment variable for worker configuration

Parameters:
- Distance: 2000m from origin point (-106.0831353, 41.5430216)
- Date Range: April 1-30, 2021 (full month)
- Attack: Constant Position Per Car 100-200m
- Features: x_pos, y_pos, coreData_elevation (ONLYXYELEV)
- Attack ratio: 30%
- Train/Test: First 100k rows train, rest test
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
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXY import CleanerWithFilterWithinRangeXY
from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
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


CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1", "train_specificity",
               "test_accuracy", "test_precision", "test_recall", "test_f1", "test_specificity"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class DaskMClassifierConstPosPerCar100To200:
    """
    Dask-based pipeline for ML classification of Connected Driving attacks.
    
    Uses distributed computing for:
    - Data loading and filtering (via Dask DataFrames)
    - Parallel data cleaning across date range
    - Memory-efficient processing of large datasets
    
    ML training still uses scikit-learn (runs after Dask processing completes).
    """
    
    @classmethod
    def getClassNameHash(cls):
        return hashlib.md5(cls.__name__.encode()).hexdigest()

    def __init__(self):
        # Setup path provider first (required for Logger dependency injection)
        self._pathprovider = PathProvider(model=self.__class__.__name__, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })
        
        self.logger = Logger(self.__class__.__name__)
        
        # Initialize Dask client if DASK_CONFIG is set
        dask_config = os.environ.get('DASK_CONFIG')
        if dask_config:
            self.logger.log(f"Loading Dask config from: {dask_config}")
            dask.config.refresh()
        
        # Create local cluster optimized for the machine's resources
        self.logger.log("Initializing Dask LocalCluster...")
        try:
            # Use local cluster with configured memory limits
            self.cluster = LocalCluster(
                n_workers=2,
                threads_per_worker=2,
                memory_limit='28GB',  # 64GB / 2 workers = 28GB each with headroom
                silence_logs=30,  # Reduce log noise
            )
            self.client = Client(self.cluster)
            self.logger.log(f"Dask dashboard available at: {self.client.dashboard_link}")
        except Exception as e:
            self.logger.log(f"Warning: Could not start Dask cluster: {e}")
            self.logger.log("Falling back to default scheduler")
            self.client = None

        #################  CONFIG FOR ALL PROPERTIES IN THE PIPELINE #################################################

        initialGathererModelName = f"{self.__class__.getClassNameHash()}-CreatingConnectedDrivingDataset"
        numSubsectionRows = -1

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
        x_pos = -106.0831353
        y_pos = 41.5430216
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-GENERATOR_PATH", contexts={
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model:  f"data/classifierdata/splitfiles/cleaned/{model}/",
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: f"data/classifierdata/splitfiles/combinedcleaned/{model}/combinedcleaned",
        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.cleandatapath
        # MDataClassifier.plot_confusion_matrix_path
        #
        self._mlPathProvider = MLPathProvider(model=self.__class__.__name__, contexts={
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
            "ConnectedDrivingLargeDataCleaner.cleanerClass": DaskCleanWithTimestamps,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": DaskCleanWithTimestamps.clean_data_with_timestamps,
            # Use DASK cleaner for distributed filtering
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
        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.columns
        # MClassifierPipeline.classifier_instances # AUTO_FILLED
        #
        # ONLYXYELEV means we only use the x_pos, y_pos, and coreData_elevation columns
        self.MLContextProvider = MLContextProvider(contexts={
            "MConnectedDrivingDataCleaner.columns": [
            "coreData_elevation",
            "x_pos", "y_pos", "isAttacker"],
            "MClassifierPipeline.csvWriter": CSVWriter(f"{self.__class__.__name__}.csv", CSV_COLUMNS),
        }
        )

        ######### END OF CONFIG FOR ALL PROPERTIES IN THE PIPELINE ##################################################

        self.csvWriter = self.MLContextProvider.get("MClassifierPipeline.csvWriter")

    def write_entire_row(self, dict):
        row = [" "]*len(CSV_COLUMNS)
        for d in dict:
            row[CSV_FORMAT[d]] = dict[d]
        self.csvWriter.addRow(row)

    def run(self):
        self.logger.log("=" * 60)
        self.logger.log("DASK PIPELINE: ConstPosPerCar 100-200m Attack Detection")
        self.logger.log("=" * 60)
        
        if self.client:
            self.logger.log(f"Workers: {len(self.client.scheduler_info()['workers'])}")
            self.logger.log(f"Total memory: {sum(w['memory_limit'] for w in self.client.scheduler_info()['workers'].values()) / 1e9:.1f} GB")
        
        self.logger.log("Starting data gathering and cleaning...")
        mcdldpgac = DaskConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

        self.logger.log("Loading all cleaned rows into memory...")
        data: DataFrame = mcdldpgac.getAllRows()
        
        total_rows = len(data)
        self.logger.log(f"Total cleaned rows: {total_rows:,}")

        # splitting into train and test sets
        seed = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED")
        numRowsToTrain = int(total_rows * 0.8)

        self.logger.log(f"Splitting data: {numRowsToTrain:,} train, {total_rows - numRowsToTrain:,} test")
        train = data.head(numRowsToTrain)
        test = data.tail(len(data)-numRowsToTrain)

        # cleaning/adding attackers to the data
        self.logger.log("Adding ConstPosPerCar 100-200m attacks to train set...")
        train = StandardPositionalOffsetAttacker(train, "train").add_attackers().add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=100, max_dist=200).get_data()
        
        self.logger.log("Adding ConstPosPerCar 100-200m attacks to test set...")
        test = StandardPositionalOffsetAttacker(test, "test").add_attackers().add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=100, max_dist=200).get_data()

        # Cleaning for ML
        self.logger.log("Cleaning data for ML pipeline...")
        mdcleaner_train = MConnectedDrivingDataCleaner(train, "train")
        mdcleaner_test = MConnectedDrivingDataCleaner(test, "test")
        m_train = mdcleaner_train.clean_data().get_cleaned_data()
        m_test = mdcleaner_test.clean_data().get_cleaned_data()

        # splitting into features and labels
        attacker_col_name = "isAttacker"
        train_X = m_train.drop(columns=[attacker_col_name])
        train_Y = m_train[attacker_col_name]
        test_X = m_test.drop(columns=[attacker_col_name])
        test_Y = m_test[attacker_col_name]

        self.logger.log(f"Training features shape: {train_X.shape}")
        self.logger.log(f"Test features shape: {test_X.shape}")

        # training the classifiers
        self.logger.log("=" * 60)
        self.logger.log("TRAINING CLASSIFIERS")
        self.logger.log("=" * 60)
        
        mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y)
        mcp.train()
        mcp.test()

        # getting the results
        results = mcp.calc_classifier_results().get_classifier_results()

        # printing the results
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
            mcp.logger.log("Timing:")
            mcp.logger.log("  Training Time: ", mclassifier.elapsed_train_time)
            mcp.logger.log("  Prediction Time: ", mclassifier.elapsed_prediction_time)

            # writing entire row to csv
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

        # calculating confusion matrices
        self.logger.log("Calculating confusion matrices...")
        mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices()
        
        # Cleanup Dask cluster
        if self.client:
            self.logger.log("Shutting down Dask cluster...")
            self.client.close()
            self.cluster.close()
        
        self.logger.log("=" * 60)
        self.logger.log("PIPELINE COMPLETE")
        self.logger.log("=" * 60)


if __name__ == "__main__":
    mcplu = DaskMClassifierConstPosPerCar100To200()
    mcplu.run()
