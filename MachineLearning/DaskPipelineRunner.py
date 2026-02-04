"""
DaskPipelineRunner - Parameterized ML Pipeline Executor

This class provides a unified, config-driven interface for running ML classifier pipelines
using Dask. It replaces the 55+ individual MClassifierLargePipeline*.py scripts with a
single parameterized runner that loads configuration from JSON files.

Usage:
    # From JSON config file
    runner = DaskPipelineRunner.from_config("configs/pipeline_2000m_rand_offset.json")
    results = runner.run()

    # From config dictionary
    config = {...}
    runner = DaskPipelineRunner(config)
    results = runner.run()

Author: Claude (Anthropic)
Date: 2026-01-18
"""

import hashlib
import json
import os
from typing import Dict, List, Any, Optional, Tuple
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Logger.Logger import DEFAULT_LOG_PATH, Logger
from MachineLearning.DaskMClassifierPipeline import DaskMClassifierPipeline
from MachineLearning.DaskMConnectedDrivingDataCleaner import DaskMConnectedDrivingDataCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider


# Default classifier instances
DEFAULT_CLASSIFIER_INSTANCES = [
    RandomForestClassifier(),
    DecisionTreeClassifier(),
    KNeighborsClassifier()
]

# CSV output format for results
CSV_COLUMNS = [
    "Model", "Total_Train_Time",
    "Total_Train_Sample_Size", "Total_Test_Sample_Size",
    "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample",
    "Prediction_Test_Set_Time_Per_Sample",
    "train_accuracy", "train_precision", "train_recall", "train_f1", "train_specificity",
    "test_accuracy", "test_precision", "test_recall", "test_f1", "test_specificity"
]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class DaskPipelineRunner:
    """
    Parameterized ML pipeline runner for Dask-based classifier training.

    Executes the complete pipeline:
    1. Data gathering (CSV → Dask DataFrame)
    2. Large data cleaning (spatial/temporal filtering)
    3. Train/test split
    4. Attack simulation (add attackers + position attacks)
    5. ML feature preparation (hex conversion, column selection)
    6. Classifier training (RandomForest, DecisionTree, KNeighbors)
    7. Results collection and output
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize DaskPipelineRunner from configuration dictionary.

        Args:
            config: Pipeline configuration with keys:
                - pipeline_name: Unique name for this pipeline
                - data: Data gathering and filtering config
                - features: Feature column selection
                - attacks: Attack simulation parameters
                - ml: ML training configuration
                - cache: Caching settings
        """
        self.config = config
        self.pipeline_name = config.get("pipeline_name", "DaskPipeline")

        # Initialize CSV writer FIRST (needed by _setup_providers for MLContextProvider)
        csv_filename = f"{self.pipeline_name}.csv"
        self.csvWriter = CSVWriter(csv_filename, CSV_COLUMNS)

        # Setup all context and path providers (Logger needs pathprovider)
        self._setup_providers()

        # Initialize logger AFTER providers are set up
        self.logger = Logger(self.pipeline_name)
        self.logger.log(f"Initializing DaskPipelineRunner: {self.pipeline_name}")
        self.logger.log("DaskPipelineRunner initialized successfully")

    @classmethod
    def from_config(cls, config_path: str) -> "DaskPipelineRunner":
        """
        Create DaskPipelineRunner from JSON config file.

        Args:
            config_path: Path to JSON configuration file

        Returns:
            DaskPipelineRunner instance
        """
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls(config)

    def _get_config_hash(self) -> str:
        """Generate MD5 hash of pipeline name for caching."""
        return hashlib.md5(self.pipeline_name.encode()).hexdigest()

    def _setup_providers(self):
        """Setup all context and path providers based on configuration."""

        config_hash = self._get_config_hash()

        # Extract config sections
        data_config = self.config.get("data", {})
        features_config = self.config.get("features", {})
        attacks_config = self.config.get("attacks", {})
        ml_config = self.config.get("ml", {})

        # PathProvider for logger
        self._pathprovider = PathProvider(
            model=self.pipeline_name,
            contexts={"Logger.logpath": DEFAULT_LOG_PATH}
        )

        # InitialGathererPathProvider for data gathering
        gatherer_model_name = f"{config_hash}-CreatingConnectedDrivingDataset"
        num_subsection_rows = data_config.get("num_subsection_rows", 100000)

        self._initialGathererPathProvider = InitialGathererPathProvider(
            model=gatherer_model_name,
            contexts={
                "DataGatherer.filepath": lambda model: data_config.get("source_file", "data/data.csv"),
                "DataGatherer.subsectionpath": lambda model: f"data/classifierdata/subsection/{model}/subsection{num_subsection_rows}.csv",
                "DataGatherer.splitfilespath": lambda model: f"data/classifierdata/splitfiles/{model}/",
            }
        )

        # GeneratorPathProvider for cleaned data paths
        self._generatorPathProvider = GeneratorPathProvider(
            model=f"{gatherer_model_name}-GENERATOR_PATH",
            contexts={
                "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model: f"data/classifierdata/splitfiles/cleaned/{model}/",
                "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: f"data/classifierdata/splitfiles/combinedcleaned/{model}/combinedcleaned",
            }
        )

        # MLPathProvider for ML output paths
        self._mlPathProvider = MLPathProvider(
            model=self.pipeline_name,
            contexts={
                "MConnectedDrivingDataCleaner.cleandatapathtrain": lambda model: f"data/mclassifierdata/cleaned/{model}/train/clean.csv",
                "MConnectedDrivingDataCleaner.cleandatapathtest": lambda model: f"data/mclassifierdata/cleaned/{model}/test/clean.csv",
                "MDataClassifier.plot_confusion_matrix_path": lambda model: f"data/mclassifierdata/results/{model}/",
            }
        )

        # GeneratorContextProvider for data processing config
        filtering_config = data_config.get("filtering", {})
        date_range_config = data_config.get("date_range", {})

        # Determine cleaner class and filter function based on filtering type
        filter_type = filtering_config.get("type", "xy_offset_position")
        cleaner_class, filter_func = self._get_cleaner_and_filter(filter_type)

        # Build context for data generation
        generator_contexts = {
            "DataGatherer.numrows": num_subsection_rows,
            "DataGatherer.lines_per_file": data_config.get("lines_per_file", 1000000),
            "ConnectedDrivingCleaner.x_pos": filtering_config.get("center_x", 0.0),
            "ConnectedDrivingCleaner.y_pos": filtering_config.get("center_y", 0.0),
            "ConnectedDrivingCleaner.columns": data_config.get("columns", self._get_default_columns()),
            "ConnectedDrivingLargeDataCleaner.max_dist": filtering_config.get("distance_meters", 2000),
            "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": cleaner_class,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": filter_func,
            "ConnectedDrivingAttacker.SEED": attacks_config.get("seed", 42),
            "ConnectedDrivingCleaner.isXYCoords": filtering_config.get("use_xy_coords", True),
            "ConnectedDrivingAttacker.attack_ratio": attacks_config.get("ratio", 0.0),
            "ConnectedDrivingCleaner.cleanParams": f"{config_hash}-CLEAN_PARAMS",
        }

        # Add date range configs if present
        if date_range_config:
            start_parts = date_range_config.get("start", "2021-01-01").split("-")
            end_parts = date_range_config.get("end", "2021-01-01").split("-")

            generator_contexts.update({
                # Config for DaskCleanerWithFilterWithinRangeXYAndDateRange
                "CleanerWithFilterWithinRangeXYAndDateRange.start_year": int(start_parts[0]),
                "CleanerWithFilterWithinRangeXYAndDateRange.start_month": int(start_parts[1]),
                "CleanerWithFilterWithinRangeXYAndDateRange.start_day": int(start_parts[2]),
                "CleanerWithFilterWithinRangeXYAndDateRange.end_year": int(end_parts[0]),
                "CleanerWithFilterWithinRangeXYAndDateRange.end_month": int(end_parts[1]),
                "CleanerWithFilterWithinRangeXYAndDateRange.end_day": int(end_parts[2]),
                # Legacy config for CleanerWithFilterWithinRangeXYAndDay (backwards compatibility)
                "CleanerWithFilterWithinRangeXYAndDay.startyear": int(start_parts[0]),
                "CleanerWithFilterWithinRangeXYAndDay.startmonth": int(start_parts[1]),
                "CleanerWithFilterWithinRangeXYAndDay.startday": int(start_parts[2]),
                "CleanerWithFilterWithinRangeXYAndDay.endyear": int(end_parts[0]),
                "CleanerWithFilterWithinRangeXYAndDay.endmonth": int(end_parts[1]),
                "CleanerWithFilterWithinRangeXYAndDay.endday": int(end_parts[2]),
            })

        # Add cleaner filter function to contexts
        if filter_type == "xy_offset_position":
            from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
            generator_contexts["ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass"] = DaskCleanerWithFilterWithinRangeXYAndDateRange
            generator_contexts["ConnectedDrivingLargeDataCleaner.filterFunc"] = DaskCleanerWithFilterWithinRangeXYAndDateRange.within_rangeXY_and_date_range

        self.generatorContextProvider = GeneratorContextProvider(contexts=generator_contexts)

        # MLContextProvider for ML-specific config
        feature_columns = features_config.get("columns", ["x_pos", "y_pos", "coreData_elevation", "isAttacker"])

        self.MLContextProvider = MLContextProvider(
            contexts={
                "MConnectedDrivingDataCleaner.columns": feature_columns,
                "MClassifierPipeline.csvWriter": self.csvWriter,
            }
        )

    def _get_default_columns(self) -> List[str]:
        """Get default BSM columns for data gathering."""
        return [
            "metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            "metadata_serialId_serialNumber", "metadata_receivedAt",
            "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_accelset_accelYaw", "coreData_speed",
            "coreData_heading", "coreData_position"
        ]

    def _get_cleaner_and_filter(self, filter_type: str) -> Tuple[Any, Any]:
        """
        Get appropriate cleaner class and filter function based on filter type.

        Args:
            filter_type: Type of filtering ("xy_offset_position", "passthrough", etc.)

        Returns:
            Tuple of (cleaner_class, filter_function)
        """
        from Generator.Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps

        if filter_type == "xy_offset_position":
            from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
            return (DaskCleanWithTimestamps, DaskCleanWithTimestamps.clean_data_with_timestamps)
        elif filter_type == "passthrough":
            from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithPassthroughFilter import DaskCleanerWithPassthroughFilter
            return (DaskCleanerWithPassthroughFilter, DaskCleanerWithPassthroughFilter.passthrough)
        else:
            # Default to timestamps cleaner
            return (DaskCleanWithTimestamps, DaskCleanWithTimestamps.clean_data_with_timestamps)

    def _apply_attacks(self, data, attack_config: Dict[str, Any], dataset_name: str):
        """
        Apply attack transformations to dataset.

        Args:
            data: Dask DataFrame to attack
            attack_config: Attack configuration dictionary
            dataset_name: Name for this dataset ("train" or "test")

        Returns:
            Dask DataFrame with attacks applied
        """
        if not attack_config.get("enabled", False):
            self.logger.log(f"Attacks disabled for {dataset_name} set")
            return data

        attack_type = attack_config.get("type", "none")

        # Initialize attacker - bypass StandardDependencyInjection by using object.__new__
        # and manually initializing the required attributes
        attacker = object.__new__(DaskConnectedDrivingAttacker)
        attacker.id = dataset_name
        attacker._pathprovider = self._generatorPathProvider
        attacker._generatorContextProvider = self.generatorContextProvider
        attacker.logger = Logger(f"DaskConnectedDrivingAttacker{dataset_name}")
        attacker.data = data
        attacker.SEED = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED", 42)
        attacker.isXYCoords = self.generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords", False)
        attacker.attack_ratio = self.generatorContextProvider.get("ConnectedDrivingAttacker.attack_ratio", 0.05)
        attacker.pos_lat_col = "y_pos"
        attacker.pos_long_col = "x_pos"
        attacker.x_col = "x_pos"
        attacker.y_col = "y_pos"

        # Add attacker labels
        attacker = attacker.add_attackers()

        # Apply position attacks based on type
        if attack_type == "rand_offset":
            min_dist = attack_config.get("min_distance", 25)
            max_dist = attack_config.get("max_distance", 250)
            attacker = attacker.add_attacks_positional_offset_rand(min_dist=min_dist, max_dist=max_dist)

        elif attack_type == "const_offset":
            direction = attack_config.get("direction_angle", 45)
            distance = attack_config.get("distance_meters", 50)
            attacker = attacker.add_attacks_positional_offset_const(direction_angle=direction, distance_meters=distance)

        elif attack_type == "const_offset_per_id":
            min_dist = attack_config.get("min_distance", 25)
            max_dist = attack_config.get("max_distance", 250)
            attacker = attacker.add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=min_dist, max_dist=max_dist)

        elif attack_type == "swap_rand":
            attacker = attacker.add_attacks_positional_swap_rand()

        elif attack_type == "override_const":
            direction = attack_config.get("direction_angle", 45)
            distance = attack_config.get("distance_meters", 50)
            attacker = attacker.add_attacks_positional_override_const(direction_angle=direction, distance_meters=distance)

        elif attack_type == "override_rand":
            min_dist = attack_config.get("min_distance", 25)
            max_dist = attack_config.get("max_distance", 250)
            attacker = attacker.add_attacks_positional_override_rand(min_dist=min_dist, max_dist=max_dist)

        return attacker.get_data()

    def write_entire_row(self, result_dict: Dict[str, Any]):
        """Write results to CSV output."""
        row = [" "] * len(CSV_COLUMNS)
        for key in result_dict:
            if key in CSV_FORMAT:
                row[CSV_FORMAT[key]] = result_dict[key]
        self.csvWriter.writeRow(row)

    def run(self) -> List[Tuple[Any, Tuple, Tuple]]:
        """
        Execute the complete ML pipeline.

        Returns:
            List of (classifier, train_results, test_results) tuples
        """
        self.logger.log("Starting DaskPipelineRunner.run()")

        # Step 1: Data gathering and cleaning
        self.logger.log("Step 1: Gathering and cleaning large dataset...")
        cleaner = DaskConnectedDrivingLargeDataCleaner(
            generatorPathProvider=self._generatorPathProvider,
            initialGathererPathProvider=self._initialGathererPathProvider,
            generatorContextProvider=self.generatorContextProvider
        )
        cleaner.clean_data()
        data = cleaner.getAllRows()

        total_rows = cleaner.getNumOfRows()
        self.logger.log(f"Total rows after cleaning: {total_rows}")

        # Step 2: Train/test split
        ml_config = self.config.get("ml", {})
        split_config = ml_config.get("train_test_split", {})

        train_ratio = split_config.get("train_ratio", 0.8)
        num_rows_to_train = int(total_rows * train_ratio) if split_config.get("type") == "random" else split_config.get("num_train_rows", 100000)

        self.logger.log(f"Step 2: Splitting into train ({num_rows_to_train}) and test ({total_rows - num_rows_to_train}) sets...")
        # Use proper Dask operations to maintain Dask DataFrame type
        # .head() returns pandas, so we need to convert back to Dask
        import dask.dataframe as dd
        
        # Compute the full dataset first, then split
        data_pd = data.compute()  # Materialize to pandas for splitting
        train_pd = data_pd.head(num_rows_to_train)
        test_pd = data_pd.tail(total_rows - num_rows_to_train) if total_rows > num_rows_to_train else data_pd.head(0)
        
        # Convert back to Dask DataFrames
        train = dd.from_pandas(train_pd, npartitions=max(1, len(train_pd) // 1000 + 1))
        test = dd.from_pandas(test_pd, npartitions=max(1, len(test_pd) // 1000 + 1)) if len(test_pd) > 0 else dd.from_pandas(train_pd.head(0), npartitions=1)

        # Step 3: Apply attacks
        attack_config = self.config.get("attacks", {})
        self.logger.log(f"Step 3: Applying attacks (enabled={attack_config.get('enabled', False)})...")
        train = self._apply_attacks(train, attack_config, "train")
        test = self._apply_attacks(test, attack_config, "test")

        # Step 4: ML feature preparation
        self.logger.log("Step 4: Preparing ML features...")
        # Bypass StandardDependencyInjection by using object.__new__ and manual init
        mdcleaner_train = object.__new__(DaskMConnectedDrivingDataCleaner)
        mdcleaner_train._MLPathProvider = self._mlPathProvider
        mdcleaner_train._MLContextprovider = self.MLContextProvider
        mdcleaner_train.suffixName = "train"
        mdcleaner_train.logger = Logger("DaskMConnectedDrivingDataCleanertrain")
        mdcleaner_train.data = train
        mdcleaner_train.cleandatapath = self._mlPathProvider.getPathWithModelName("MConnectedDrivingDataCleaner.cleandatapathtrain")
        mdcleaner_train.columns = self.MLContextProvider.get("MConnectedDrivingDataCleaner.columns")
        
        mdcleaner_test = object.__new__(DaskMConnectedDrivingDataCleaner)
        mdcleaner_test._MLPathProvider = self._mlPathProvider
        mdcleaner_test._MLContextprovider = self.MLContextProvider
        mdcleaner_test.suffixName = "test"
        mdcleaner_test.logger = Logger("DaskMConnectedDrivingDataCleanertest")
        mdcleaner_test.data = test
        mdcleaner_test.cleandatapath = self._mlPathProvider.getPathWithModelName("MConnectedDrivingDataCleaner.cleandatapathtest")
        mdcleaner_test.columns = self.MLContextProvider.get("MConnectedDrivingDataCleaner.columns")

        m_train = mdcleaner_train.clean_data().get_cleaned_data()
        m_test = mdcleaner_test.clean_data().get_cleaned_data()

        # Step 5: Split features and labels
        self.logger.log("Step 5: Splitting features and labels...")
        attacker_col_name = "isAttacker"
        train_X = m_train.drop(columns=[attacker_col_name])
        train_Y = m_train[attacker_col_name]
        test_X = m_test.drop(columns=[attacker_col_name])
        test_Y = m_test[attacker_col_name]

        # Step 6: Train classifiers
        self.logger.log("Step 6: Training classifiers...")
        # Bypass StandardDependencyInjection - initialize manually
        mcp = object.__new__(DaskMClassifierPipeline)
        mcp._pathprovider = self._mlPathProvider
        mcp._MLContextProvider = self.MLContextProvider
        mcp.logger = Logger("DaskMClassifierPipeline")
        
        # Get classifier instances from configuration
        from MachineLearning.DaskMClassifierPipeline import DEFAULT_CLASSIFIER_INSTANCES
        mcp.classifier_instances = self.MLContextProvider.get(
            "MClassifierPipeline.classifier_instances",
            DEFAULT_CLASSIFIER_INSTANCES
        )
        
        # Convert Dask DataFrames to pandas for sklearn
        mcp.logger.log("Converting input data to pandas (if needed)...")
        train_X_pd = train_X.compute() if hasattr(train_X, 'compute') else train_X
        train_Y_pd = train_Y.compute() if hasattr(train_Y, 'compute') else train_Y
        test_X_pd = test_X.compute() if hasattr(test_X, 'compute') else test_X
        test_Y_pd = test_Y.compute() if hasattr(test_Y, 'compute') else test_Y
        mcp.logger.log("Data conversion complete. Creating classifiers...")
        
        # Initialize storage and classifiers
        mcp.classifiers_and_confusion_matrices = []
        mcp.classifiers = []
        from MachineLearning.MDataClassifier import MDataClassifier
        for classifier_instance in mcp.classifier_instances:
            classifier_name = classifier_instance.__class__.__name__
            mcp.logger.log(f"Creating MDataClassifier for {classifier_name}...")
            mcp.classifiers.append(
                MDataClassifier(classifier_instance, train_X_pd, train_Y_pd, test_X_pd, test_Y_pd)
            )
        mcp.logger.log(f"Initialized {len(mcp.classifiers)} classifiers")

        mcp.train()
        mcp.test()

        # Step 7: Calculate results
        self.logger.log("Step 7: Calculating classifier results...")
        results = mcp.calc_classifier_results().get_classifier_results()

        # Step 8: Log and save results
        self.logger.log("Step 8: Logging results...")
        for mclassifier, train_result, test_result in results:
            self.logger.log(f"\n{mclassifier}")
            self.logger.log("Train Set Results:")
            self.logger.log(f"  Accuracy: {train_result[0]}")
            self.logger.log(f"  Precision: {train_result[1]}")
            self.logger.log(f"  Recall: {train_result[2]}")
            self.logger.log(f"  F1: {train_result[3]}")
            self.logger.log(f"  Specificity: {train_result[4]}")

            self.logger.log("Test Set Results:")
            self.logger.log(f"  Accuracy: {test_result[0]}")
            self.logger.log(f"  Precision: {test_result[1]}")
            self.logger.log(f"  Recall: {test_result[2]}")
            self.logger.log(f"  F1: {test_result[3]}")
            self.logger.log(f"  Specificity: {test_result[4]}")

            # Write to CSV
            self.write_entire_row({
                "Model": str(mclassifier),
                "train_accuracy": train_result[0],
                "train_precision": train_result[1],
                "train_recall": train_result[2],
                "train_f1": train_result[3],
                "train_specificity": train_result[4],
                "test_accuracy": test_result[0],
                "test_precision": test_result[1],
                "test_recall": test_result[2],
                "test_f1": test_result[3],
                "test_specificity": test_result[4],
            })

        self.logger.log("DaskPipelineRunner.run() completed successfully")
        return results


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) < 2:
        print("Usage: python DaskPipelineRunner.py <config_file.json>")
        sys.exit(1)

    config_file = sys.argv[1]
    runner = DaskPipelineRunner.from_config(config_file)
    results = runner.run()

    print(f"\nCompleted {len(results)} classifier trainings")
