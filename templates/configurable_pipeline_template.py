#!/usr/bin/env python3
"""
Configurable Pipeline Template for Connected Driving Simulation Matrix

This template supports all spatial radii (200km, 100km, 2km) and feature sets 
(BASIC, BASIC_WITH_ID, MOVEMENT, MOVEMENT_WITH_ID, EXTENDED, EXTENDED_WITH_ID).

Usage:
    python configurable_pipeline_template.py --config <config_file>
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

# Dask imports
import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

# Pipeline imports
from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Helpers.MathHelper import MathHelper
from Logger.Logger import DEFAULT_LOG_PATH, Logger
from Generator.Cleaners.DaskConnectedDrivingLargeDataPipelineGathererAndCleaner import DaskConnectedDrivingLargeDataPipelineGathererAndCleaner
from MachineLearning.MClassifierPipeline import MClassifierPipeline
from MachineLearning.MConnectedDrivingDataCleaner import MConnectedDrivingDataCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider


class ConfigurablePipeline:
    """Configurable pipeline that adapts based on configuration parameters."""
    
    def __init__(self, config_file: str):
        """Initialize with configuration file."""
        self.config_file = config_file
        self.config = self._load_config()
        self.logger = None
        self.client = None
        
    def _load_config(self) -> dict:
        """Load pipeline configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _setup_logging(self):
        """Setup logging based on configuration."""
        log_dir = self.config.get('output', {}).get('log_dir', 'logs/')
        os.makedirs(log_dir, exist_ok=True)
        
        pipeline_name = self.config.get('pipeline_name', 'pipeline')
        log_file = os.path.join(log_dir, f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        self.logger = Logger(log_file)
        self.logger.log(f"Pipeline started: {pipeline_name}")
        self.logger.log(f"Configuration: {self.config_file}")
        
    def _setup_dask(self):
        """Setup Dask cluster based on configuration."""
        dask_config = self.config.get('dask', {})
        n_workers = dask_config.get('n_workers', 4)
        threads_per_worker = dask_config.get('threads_per_worker', 2)
        memory_limit = dask_config.get('memory_limit', '12GB')
        
        self.logger.log(f"Setting up Dask cluster: {n_workers} workers, {threads_per_worker} threads/worker, {memory_limit} memory limit")
        
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            dashboard_address=None
        )
        
        self.client = Client(cluster)
        self.logger.log(f"Dask cluster started: {self.client.dashboard_link}")
        
    def _create_output_directories(self):
        """Create output directories based on configuration."""
        output_config = self.config.get('output', {})
        results_dir = output_config.get('results_dir', 'results/')
        log_dir = output_config.get('log_dir', 'logs/')
        
        cache_config = self.config.get('cache', {})
        for cache_path in cache_config.values():
            cache_dir = os.path.dirname(cache_path)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
        
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        
        self.logger.log(f"Output directories created: {results_dir}, {log_dir}")
    
    def _get_data_cleaner(self):
        """Create data cleaner based on configuration."""
        data_config = self.config['data']
        
        source_file = data_config['source_file']
        columns_to_extract = data_config['columns_to_extract']
        filtering = data_config['filtering']
        date_range = data_config['date_range']
        
        self.logger.log(f"Creating data cleaner for {source_file}")
        self.logger.log(f"Columns to extract: {columns_to_extract}")
        self.logger.log(f"Spatial filter: {filtering['radius_meters']}m radius from ({filtering['center_latitude']}, {filtering['center_longitude']})")
        self.logger.log(f"Date range: {date_range['start']} to {date_range['end']}")
        
        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange(
            source_file,
            columns_to_extract,
            filtering['center_longitude'],
            filtering['center_latitude'],
            filtering['radius_meters'],
            date_range['start'],
            date_range['end']
        )
        
        return cleaner
    
    def _get_attacker(self, clean_data):
        """Create attacker based on configuration."""
        attack_config = self.config['attack']
        attack_type = attack_config['type']
        malicious_ratio = attack_config['malicious_ratio']
        
        self.logger.log(f"Creating attacker: {attack_type} with {malicious_ratio*100}% malicious ratio")
        
        if attack_type == "constant_offset_per_vehicle":
            offset_min = attack_config.get('offset_distance_min', 100)
            offset_max = attack_config.get('offset_distance_max', 200)
            direction_min = attack_config.get('offset_direction_min', 0)
            direction_max = attack_config.get('offset_direction_max', 360)
            seed = attack_config.get('seed', 42)
            
            self.logger.log(f"Offset parameters: {offset_min}-{offset_max}m distance, {direction_min}-{direction_max}° direction, seed={seed}")
            
            attacker = StandardPositionalOffsetAttacker(
                clean_data,
                malicious_ratio,
                offset_min,
                offset_max,
                direction_min,
                direction_max,
                seed
            )
        
        else:
            raise NotImplementedError(f"Attack type '{attack_type}' not implemented yet")
        
        return attacker
    
    def _get_classifiers(self):
        """Create ML classifiers based on configuration."""
        ml_config = self.config['ml']
        classifier_names = ml_config.get('classifiers', ['RandomForest'])
        
        classifiers = []
        
        for name in classifier_names:
            if name == "RandomForest":
                classifiers.append(("RandomForest", RandomForestClassifier(random_state=42)))
            elif name == "DecisionTree":
                classifiers.append(("DecisionTree", DecisionTreeClassifier(random_state=42)))
            elif name == "KNeighbors":
                classifiers.append(("KNeighbors", KNeighborsClassifier()))
            else:
                self.logger.log(f"Warning: Unknown classifier '{name}' skipped")
        
        self.logger.log(f"Created classifiers: {[name for name, _ in classifiers]}")
        return classifiers
    
    def run_pipeline(self):
        """Run the complete pipeline based on configuration."""
        
        self.logger.log("="*60)
        self.logger.log("STARTING CONFIGURABLE PIPELINE")
        self.logger.log("="*60)
        
        # Log configuration summary
        if 'template_parameters' in self.config:
            params = self.config['template_parameters']
            self.logger.log(f"Template Parameters:")
            self.logger.log(f"  Spatial Radius: {params.get('spatial_radius', 'N/A')}")
            self.logger.log(f"  Feature Set: {params.get('feature_set', 'N/A')}")
            self.logger.log(f"  Attack Type: {params.get('attack_type', 'N/A')}")
            self.logger.log(f"  Malicious Ratio: {params.get('malicious_ratio', 'N/A')}")
        
        try:
            # Step 1: Clean data
            self.logger.log("\n" + "="*40)
            self.logger.log("STEP 1: DATA CLEANING")
            self.logger.log("="*40)
            
            cleaner = self._get_data_cleaner()
            clean_data = cleaner.clean()
            
            self.logger.log(f"Data cleaning completed. Records: {len(clean_data) if hasattr(clean_data, '__len__') else 'Unknown'}")
            
            # Step 2: Apply attacks
            self.logger.log("\n" + "="*40)
            self.logger.log("STEP 2: APPLYING ATTACKS")
            self.logger.log("="*40)
            
            attacker = self._get_attacker(clean_data)
            attack_data = attacker.attack()
            
            self.logger.log(f"Attack application completed. Records: {len(attack_data) if hasattr(attack_data, '__len__') else 'Unknown'}")
            
            # Step 3: Machine Learning
            self.logger.log("\n" + "="*40)
            self.logger.log("STEP 3: MACHINE LEARNING")
            self.logger.log("="*40)
            
            ml_config = self.config['ml']
            features = ml_config['features']
            label = ml_config['label']
            
            self.logger.log(f"ML Features: {features}")
            self.logger.log(f"ML Label: {label}")
            
            # Prepare ML data
            if hasattr(attack_data, 'compute'):
                # Dask DataFrame
                ml_data = attack_data[features + [label]].compute()
            else:
                # Pandas DataFrame
                ml_data = attack_data[features + [label]]
            
            X = ml_data[features]
            y = ml_data[label]
            
            # Train/test split
            split_config = ml_config.get('train_test_split', {})
            test_size = split_config.get('test_size', 0.20)
            random_state = split_config.get('random_state', 42)
            shuffle = split_config.get('shuffle', True)
            
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=test_size,
                random_state=random_state,
                shuffle=shuffle
            )
            
            self.logger.log(f"Train/test split: {len(X_train)} train, {len(X_test)} test")
            
            # Train and evaluate classifiers
            classifiers = self._get_classifiers()
            results = []
            
            for name, classifier in classifiers:
                self.logger.log(f"\nTraining {name}...")
                
                classifier.fit(X_train, y_train)
                train_score = classifier.score(X_train, y_train)
                test_score = classifier.score(X_test, y_test)
                
                result = {
                    'classifier': name,
                    'train_accuracy': train_score,
                    'test_accuracy': test_score,
                    'features': features,
                    'feature_count': len(features)
                }
                
                results.append(result)
                
                self.logger.log(f"{name} Results:")
                self.logger.log(f"  Train Accuracy: {train_score:.4f}")
                self.logger.log(f"  Test Accuracy: {test_score:.4f}")
            
            # Save results
            self._save_results(results)
            
            self.logger.log("\n" + "="*40)
            self.logger.log("PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.log("="*40)
            
            return results
            
        except Exception as e:
            self.logger.log(f"\n❌ PIPELINE FAILED: {str(e)}")
            raise
        
        finally:
            if self.client:
                self.client.close()
                self.logger.log("Dask client closed")
    
    def _save_results(self, results):
        """Save pipeline results to file."""
        output_config = self.config.get('output', {})
        results_dir = output_config.get('results_dir', 'results/')
        
        pipeline_name = self.config.get('pipeline_name', 'pipeline')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        results_file = os.path.join(results_dir, f"{pipeline_name}_results_{timestamp}.json")
        
        results_data = {
            'pipeline_name': pipeline_name,
            'config_file': self.config_file,
            'timestamp': timestamp,
            'template_parameters': self.config.get('template_parameters', {}),
            'results': results
        }
        
        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        self.logger.log(f"Results saved to: {results_file}")


def main():
    """CLI interface for configurable pipeline."""
    parser = argparse.ArgumentParser(description="Run configurable Connected Driving pipeline")
    parser.add_argument("--config", "-c", required=True, help="Pipeline configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without running")
    
    args = parser.parse_args()
    
    try:
        pipeline = ConfigurablePipeline(args.config)
        
        if args.dry_run:
            print(f"✅ Configuration valid: {args.config}")
            print(f"Pipeline name: {pipeline.config.get('pipeline_name', 'N/A')}")
            if 'template_parameters' in pipeline.config:
                params = pipeline.config['template_parameters']
                print(f"Template parameters:")
                for key, value in params.items():
                    print(f"  {key}: {value}")
            return
        
        pipeline._setup_logging()
        pipeline._setup_dask()
        pipeline._create_output_directories()
        
        results = pipeline.run_pipeline()
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"Results:")
        for result in results:
            print(f"  {result['classifier']}: Train={result['train_accuracy']:.4f}, Test={result['test_accuracy']:.4f}")
    
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
