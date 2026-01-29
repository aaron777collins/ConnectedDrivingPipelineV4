#!/usr/bin/env python3
"""
Full End-to-End Pipeline Test with ML

This script runs the complete ConnectedDrivingPipelineV4 workflow:
1. Generate synthetic BSM data with attack patterns
2. Validate and cache the data
3. Feature engineering
4. Train ML models for attack detection
5. Evaluate and analyze results

Usage:
    python scripts/full_e2e_pipeline.py
"""

import sys
import os
import json
import tempfile
import time
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
import yaml

# Dask
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd

# Sklearn
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, roc_auc_score
)

# Project imports
from DataSources.CacheManager import CacheManager
from DataSources.SchemaValidator import BSMSchemaValidator
from DataSources.SyntheticDataGenerator import SyntheticBSMGenerator
from DataSources.config import DataSourceConfig, DateRangeConfig


class AttackSimulator:
    """Simulate various attack patterns in BSM data."""
    
    def __init__(self, seed: int = 42):
        self.rng = np.random.default_rng(seed)
        self.attack_types = ['normal', 'speed_attack', 'position_attack', 'replay_attack']
    
    def inject_attacks(self, df: pd.DataFrame, attack_ratio: float = 0.15) -> pd.DataFrame:
        """
        Inject attack patterns into the dataset.
        
        Args:
            df: Clean BSM data
            attack_ratio: Fraction of records to attack (default 15%)
        
        Returns:
            DataFrame with attack_label column and modified values
        """
        df = df.copy()
        n = len(df)
        n_attacks = int(n * attack_ratio)
        
        # Initialize all as normal
        df['attack_label'] = 0
        df['attack_type'] = 'normal'
        
        # Select random indices for attacks
        attack_indices = self.rng.choice(n, size=n_attacks, replace=False)
        
        # Distribute among attack types (excluding normal)
        attack_type_indices = np.array_split(attack_indices, 3)
        
        # Speed attacks: Unrealistic speed values
        speed_attack_idx = attack_type_indices[0]
        df.loc[speed_attack_idx, 'speed'] = df.loc[speed_attack_idx, 'speed'] * self.rng.uniform(3, 10, len(speed_attack_idx))
        df.loc[speed_attack_idx, 'attack_label'] = 1
        df.loc[speed_attack_idx, 'attack_type'] = 'speed_attack'
        
        # Position attacks: Jump to impossible locations
        position_attack_idx = attack_type_indices[1]
        df.loc[position_attack_idx, 'latitude'] += self.rng.uniform(0.5, 2.0, len(position_attack_idx)) * self.rng.choice([-1, 1], len(position_attack_idx))
        df.loc[position_attack_idx, 'attack_label'] = 1
        df.loc[position_attack_idx, 'attack_type'] = 'position_attack'
        
        # Replay attacks: Duplicate timestamps with slight variations
        replay_attack_idx = attack_type_indices[2]
        df.loc[replay_attack_idx, 'heading'] = df.loc[replay_attack_idx, 'heading'] + self.rng.uniform(-180, 180, len(replay_attack_idx))
        df.loc[replay_attack_idx, 'heading'] = df.loc[replay_attack_idx, 'heading'] % 360
        df.loc[replay_attack_idx, 'attack_label'] = 1
        df.loc[replay_attack_idx, 'attack_type'] = 'replay_attack'
        
        return df


class FeatureEngineer:
    """Feature engineering for BSM attack detection."""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
    
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create derived features from raw BSM data."""
        df = df.copy()
        
        # Sort by vehicle and timestamp for sequential features
        if 'vehicle_id' in df.columns:
            df = df.sort_values(['vehicle_id', 'timestamp'])
        
        # Speed features
        df['speed_zscore'] = (df['speed'] - df['speed'].mean()) / df['speed'].std()
        df['speed_clipped'] = df['speed'].clip(0, 50)  # Reasonable speed range
        
        # Position features
        df['lat_zscore'] = (df['latitude'] - df['latitude'].mean()) / df['latitude'].std()
        df['lon_zscore'] = (df['longitude'] - df['longitude'].mean()) / df['longitude'].std()
        
        # Heading features
        df['heading_sin'] = np.sin(np.radians(df['heading']))
        df['heading_cos'] = np.cos(np.radians(df['heading']))
        
        # Elevation features
        df['elevation_normalized'] = (df['elevation'] - df['elevation'].min()) / (df['elevation'].max() - df['elevation'].min() + 1e-6)
        
        # Anomaly indicators
        df['is_speed_outlier'] = (np.abs(df['speed_zscore']) > 3).astype(int)
        df['is_position_outlier'] = ((np.abs(df['lat_zscore']) > 3) | (np.abs(df['lon_zscore']) > 3)).astype(int)
        
        return df
    
    def prepare_ml_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare feature matrix and labels for ML."""
        
        feature_cols = [
            'latitude', 'longitude', 'elevation', 'speed', 'heading',
            'speed_zscore', 'speed_clipped', 'lat_zscore', 'lon_zscore',
            'heading_sin', 'heading_cos', 'elevation_normalized',
            'is_speed_outlier', 'is_position_outlier'
        ]
        
        # Filter to available features
        available_features = [c for c in feature_cols if c in df.columns]
        
        X = df[available_features].fillna(0).values
        y = df['attack_label'].values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        return X_scaled, y, available_features


class E2EPipeline:
    """Full end-to-end pipeline execution."""
    
    def __init__(self, temp_dir: Path, ml_config: Dict):
        self.temp_dir = temp_dir
        self.ml_config = ml_config
        self.cache_dir = temp_dir / "cache"
        self.results_dir = temp_dir / "results"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        self.results = {}
        self.models = {}
    
    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = {"INFO": "✓", "WARN": "⚠", "ERROR": "✗", "START": "→", "DATA": "📊"}.get(level, " ")
        print(f"[{ts}] {prefix} {msg}")
    
    def step1_generate_data(self, n_days: int = 3, records_per_day: int = 5000) -> pd.DataFrame:
        """Step 1: Generate synthetic BSM data."""
        self.log("STEP 1: Generating synthetic BSM data...", "START")
        
        gen = SyntheticBSMGenerator(seed=42, num_vehicles=50)
        validator = BSMSchemaValidator()
        
        all_records = []
        
        for day in range(n_days):
            target_date = date(2021, 4, 1) + timedelta(days=day)
            
            # Generate hourly batches
            for hour in range(24):
                timestamp = datetime(2021, 4, 1 + day, hour, 0, 0)
                batch = gen.generate_batch(timestamp, num_records=records_per_day // 24)
                all_records.extend(batch)
        
        self.log(f"Generated {len(all_records)} raw BSM records")
        
        # Validate
        report = validator.batch_validate(all_records)
        self.log(f"Validation: {report.valid_count}/{report.total_count} valid ({100*report.valid_count/report.total_count:.1f}%)")
        
        # Flatten to DataFrame
        flat_records = []
        for r in all_records:
            try:
                flat = {
                    'timestamp': r['metadata']['recordGeneratedAt'],
                    'latitude': r['payload']['data']['coreData']['position']['latitude'],
                    'longitude': r['payload']['data']['coreData']['position']['longitude'],
                    'elevation': r['payload']['data']['coreData']['position']['elevation'],
                    'speed': r['payload']['data']['coreData']['speed'],
                    'heading': r['payload']['data']['coreData']['heading'],
                    'vehicle_id': r['payload']['data']['coreData']['id']
                }
                flat_records.append(flat)
            except (KeyError, TypeError):
                continue
        
        df = pd.DataFrame(flat_records)
        self.log(f"Flattened to DataFrame: {len(df)} rows, {len(df.columns)} columns")
        
        self.results['step1'] = {
            'total_records': len(all_records),
            'valid_records': report.valid_count,
            'flattened_rows': len(df)
        }
        
        return df
    
    def step2_inject_attacks(self, df: pd.DataFrame, attack_ratio: float = 0.15) -> pd.DataFrame:
        """Step 2: Inject attack patterns."""
        self.log("STEP 2: Injecting attack patterns...", "START")
        
        simulator = AttackSimulator(seed=123)
        df_attacked = simulator.inject_attacks(df, attack_ratio=attack_ratio)
        
        attack_counts = df_attacked['attack_type'].value_counts()
        self.log(f"Attack distribution:", "DATA")
        for attack_type, count in attack_counts.items():
            pct = 100 * count / len(df_attacked)
            self.log(f"  - {attack_type}: {count} ({pct:.1f}%)")
        
        self.results['step2'] = {
            'total_records': len(df_attacked),
            'attack_ratio': attack_ratio,
            'attack_distribution': attack_counts.to_dict()
        }
        
        return df_attacked
    
    def step3_cache_data(self, df: pd.DataFrame) -> None:
        """Step 3: Cache data for ML processing."""
        self.log("STEP 3: Caching data...", "START")
        
        cache = CacheManager(
            cache_dir=self.cache_dir,
            max_size_gb=2.0,
            verify_checksums=True
        )
        
        # Save full dataset
        cache.save_processed(df, "wydot", "BSM", date(2021, 4, 1))
        
        entry = cache.get_entry("wydot", "BSM", date(2021, 4, 1))
        stats = cache.get_stats()
        
        self.log(f"Cached {entry.row_count} rows ({stats['total_size_gb']:.3f}GB)")
        
        self.results['step3'] = {
            'cached_rows': entry.row_count,
            'cache_size_gb': stats['total_size_gb'],
            'cache_status': entry.status
        }
    
    def step4_feature_engineering(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Step 4: Feature engineering."""
        self.log("STEP 4: Feature engineering...", "START")
        
        engineer = FeatureEngineer()
        df_features = engineer.create_features(df)
        X, y, feature_names = engineer.prepare_ml_data(df_features)
        
        self.log(f"Created {len(feature_names)} features for {len(X)} samples")
        self.log(f"Class distribution: {np.bincount(y.astype(int))}")
        
        self.results['step4'] = {
            'n_samples': len(X),
            'n_features': len(feature_names),
            'feature_names': feature_names,
            'class_counts': np.bincount(y.astype(int)).tolist()
        }
        
        # Store for later
        self.feature_engineer = engineer
        
        return X, y, feature_names
    
    def step5_train_models(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Step 5: Train ML models."""
        self.log("STEP 5: Training ML models...", "START")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        self.log(f"Train: {len(X_train)}, Test: {len(X_test)}")
        
        model_results = {}
        
        # Model 1: Random Forest
        self.log("Training Random Forest...", "START")
        rf_params = self.ml_config['models']['random_forest']['params']
        rf = RandomForestClassifier(
            n_estimators=rf_params.get('n_estimators', 100),
            max_depth=rf_params.get('max_depth', 15),
            n_jobs=-1,
            random_state=42
        )
        
        # Cross-validation
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        rf_cv_scores = cross_val_score(rf, X_train, y_train, cv=cv, scoring='f1')
        
        # Fit final model
        rf.fit(X_train, y_train)
        rf_pred = rf.predict(X_test)
        rf_proba = rf.predict_proba(X_test)[:, 1]
        
        model_results['random_forest'] = {
            'cv_f1_mean': float(rf_cv_scores.mean()),
            'cv_f1_std': float(rf_cv_scores.std()),
            'test_accuracy': float(accuracy_score(y_test, rf_pred)),
            'test_precision': float(precision_score(y_test, rf_pred)),
            'test_recall': float(recall_score(y_test, rf_pred)),
            'test_f1': float(f1_score(y_test, rf_pred)),
            'test_roc_auc': float(roc_auc_score(y_test, rf_proba)),
            'confusion_matrix': confusion_matrix(y_test, rf_pred).tolist()
        }
        
        self.log(f"Random Forest: F1={model_results['random_forest']['test_f1']:.3f}, AUC={model_results['random_forest']['test_roc_auc']:.3f}")
        
        # Model 2: Gradient Boosting
        self.log("Training Gradient Boosting...", "START")
        gb_params = self.ml_config['models']['gradient_boosting']['params']
        gb = GradientBoostingClassifier(
            n_estimators=gb_params.get('n_estimators', 100),
            learning_rate=gb_params.get('learning_rate', 0.1),
            max_depth=gb_params.get('max_depth', 5),
            random_state=42
        )
        
        gb_cv_scores = cross_val_score(gb, X_train, y_train, cv=cv, scoring='f1')
        gb.fit(X_train, y_train)
        gb_pred = gb.predict(X_test)
        gb_proba = gb.predict_proba(X_test)[:, 1]
        
        model_results['gradient_boosting'] = {
            'cv_f1_mean': float(gb_cv_scores.mean()),
            'cv_f1_std': float(gb_cv_scores.std()),
            'test_accuracy': float(accuracy_score(y_test, gb_pred)),
            'test_precision': float(precision_score(y_test, gb_pred)),
            'test_recall': float(recall_score(y_test, gb_pred)),
            'test_f1': float(f1_score(y_test, gb_pred)),
            'test_roc_auc': float(roc_auc_score(y_test, gb_proba)),
            'confusion_matrix': confusion_matrix(y_test, gb_pred).tolist()
        }
        
        self.log(f"Gradient Boosting: F1={model_results['gradient_boosting']['test_f1']:.3f}, AUC={model_results['gradient_boosting']['test_roc_auc']:.3f}")
        
        # Store models
        self.models = {'random_forest': rf, 'gradient_boosting': gb}
        self.results['step5'] = model_results
        
        return model_results
    
    def step6_analyze_results(self) -> Dict[str, Any]:
        """Step 6: Analyze and report results."""
        self.log("STEP 6: Analyzing results...", "START")
        
        analysis = {
            'best_model': None,
            'best_f1': 0,
            'recommendations': []
        }
        
        for model_name, metrics in self.results['step5'].items():
            if metrics['test_f1'] > analysis['best_f1']:
                analysis['best_f1'] = metrics['test_f1']
                analysis['best_model'] = model_name
        
        self.log(f"Best model: {analysis['best_model']} (F1={analysis['best_f1']:.3f})", "DATA")
        
        # Feature importance from best model
        best_model = self.models[analysis['best_model']]
        feature_importance = dict(zip(
            self.results['step4']['feature_names'],
            best_model.feature_importances_
        ))
        
        # Sort by importance
        sorted_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        
        self.log("Top 5 features:", "DATA")
        for feat, imp in sorted_importance[:5]:
            self.log(f"  - {feat}: {imp:.4f}")
        
        analysis['feature_importance'] = {k: float(v) for k, v in sorted_importance}
        
        # Recommendations
        best_metrics = self.results['step5'][analysis['best_model']]
        
        if best_metrics['test_f1'] >= 0.9:
            analysis['recommendations'].append("Excellent attack detection performance!")
        elif best_metrics['test_f1'] >= 0.7:
            analysis['recommendations'].append("Good performance. Consider hyperparameter tuning.")
        else:
            analysis['recommendations'].append("Performance could be improved. Consider more features or data.")
        
        if best_metrics['test_precision'] < best_metrics['test_recall']:
            analysis['recommendations'].append("Model favors recall over precision (catches more attacks but more false positives)")
        else:
            analysis['recommendations'].append("Model favors precision over recall (fewer false positives but might miss some attacks)")
        
        self.results['step6'] = analysis
        
        return analysis
    
    def run(self, n_days: int = 3, records_per_day: int = 5000) -> Dict[str, Any]:
        """Run the full pipeline."""
        print("\n" + "="*70)
        print("  ConnectedDrivingPipelineV4 - Full End-to-End ML Pipeline")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        # Execute pipeline steps
        df = self.step1_generate_data(n_days, records_per_day)
        print()
        
        df_attacked = self.step2_inject_attacks(df)
        print()
        
        self.step3_cache_data(df_attacked)
        print()
        
        X, y, features = self.step4_feature_engineering(df_attacked)
        print()
        
        self.step5_train_models(X, y)
        print()
        
        analysis = self.step6_analyze_results()
        print()
        
        elapsed = time.time() - start_time
        
        # Final summary
        print("="*70)
        print("  PIPELINE SUMMARY")
        print("="*70)
        print(f"\n  Total time: {elapsed:.1f}s")
        print(f"  Data: {self.results['step1']['flattened_rows']} records")
        print(f"  Features: {self.results['step4']['n_features']}")
        print(f"  Best model: {analysis['best_model']}")
        print(f"  Best F1: {analysis['best_f1']:.3f}")
        print(f"\n  Recommendations:")
        for rec in analysis['recommendations']:
            print(f"    - {rec}")
        print("\n" + "="*70)
        
        self.results['summary'] = {
            'elapsed_seconds': elapsed,
            'success': True
        }
        
        return self.results


def main():
    """Main entry point."""
    # Load ML config
    config_path = Path(__file__).parent.parent / "configs" / "ml" / "32gb-production.yml"
    with open(config_path) as f:
        ml_config = yaml.safe_load(f)
    
    print(f"Loaded ML config from {config_path}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        pipeline = E2EPipeline(Path(tmpdir), ml_config)
        
        # Run with moderate data size
        results = pipeline.run(n_days=3, records_per_day=6000)
        
        # Save results
        results_file = Path(tmpdir) / "results" / "pipeline_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\nResults saved to: {results_file}")
        
        # Verify results
        assert results['summary']['success'], "Pipeline failed!"
        assert results['step6']['best_f1'] > 0.5, "Model performance too low!"
        
        print("\n✅ Full E2E pipeline completed successfully!")
        
        return 0


if __name__ == "__main__":
    sys.exit(main())
