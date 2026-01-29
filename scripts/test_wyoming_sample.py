#!/usr/bin/env python3
"""
Test pipeline with real Wyoming BSM sample data.

This script:
1. Loads the wyoming100M.csv sample data
2. Converts to our standard format
3. Runs through validation, caching, and ML pipeline
4. Reports results
"""

import sys
import os
import tempfile
import time
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import yaml
import warnings
warnings.filterwarnings('ignore')

# Sklearn
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

# Project imports
from DataSources.CacheManager import CacheManager
from DataSources.config import DataSourceConfig, DateRangeConfig


class WyomingSamplePipeline:
    """Pipeline for real Wyoming BSM sample data."""
    
    def __init__(self, data_path: Path, cache_dir: Path):
        self.data_path = data_path
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.results = {}
    
    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = {"INFO": "✓", "WARN": "⚠", "ERROR": "✗", "START": "→", "DATA": "📊"}.get(level, " ")
        print(f"[{ts}] {prefix} {msg}")
    
    def step1_load_data(self) -> pd.DataFrame:
        """Load and parse the Wyoming CSV data."""
        self.log("STEP 1: Loading Wyoming sample data...", "START")
        
        # Read CSV with specific dtypes to handle mixed data
        df = pd.read_csv(self.data_path, low_memory=False)
        
        self.log(f"Loaded {len(df)} records with {len(df.columns)} columns")
        
        # Extract relevant columns and rename
        column_mapping = {
            'metadata_generatedAt': 'timestamp',
            'coreData_position_lat': 'latitude',
            'coreData_position_long': 'longitude', 
            'coreData_elevation': 'elevation',
            'coreData_speed': 'speed',
            'coreData_heading': 'heading',
            'coreData_id': 'vehicle_id'
        }
        
        # Check which columns exist
        available_cols = [c for c in column_mapping.keys() if c in df.columns]
        self.log(f"Found {len(available_cols)}/{len(column_mapping)} expected columns")
        
        # Create simplified dataframe
        df_clean = df[available_cols].rename(columns=column_mapping)
        
        # Convert numeric columns
        numeric_cols = ['latitude', 'longitude', 'elevation', 'speed', 'heading']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Remove rows with missing critical values
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['latitude', 'longitude', 'speed'])
        self.log(f"After cleaning: {len(df_clean)} records ({initial_count - len(df_clean)} dropped)")
        
        self.results['step1'] = {
            'total_loaded': len(df),
            'after_cleaning': len(df_clean),
            'columns': list(df_clean.columns)
        }
        
        return df_clean
    
    def step2_analyze_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze and describe the data."""
        self.log("STEP 2: Analyzing data distribution...", "START")
        
        stats = {}
        for col in ['latitude', 'longitude', 'elevation', 'speed', 'heading']:
            if col in df.columns:
                stats[col] = {
                    'min': float(df[col].min()),
                    'max': float(df[col].max()),
                    'mean': float(df[col].mean()),
                    'std': float(df[col].std())
                }
                self.log(f"  {col}: min={stats[col]['min']:.2f}, max={stats[col]['max']:.2f}, mean={stats[col]['mean']:.2f}")
        
        # Check for outliers
        if 'speed' in df.columns:
            high_speed = (df['speed'] > 50).sum()  # > 50 m/s = > 112 mph
            self.log(f"  High speed records (>50 m/s): {high_speed} ({100*high_speed/len(df):.2f}%)")
        
        if 'latitude' in df.columns:
            # Wyoming bounds roughly: lat 41-45, lon -111 to -104
            out_of_bounds = ((df['latitude'] < 40) | (df['latitude'] > 46) |
                           (df['longitude'] < -112) | (df['longitude'] > -103)).sum()
            self.log(f"  Out of Wyoming bounds: {out_of_bounds} ({100*out_of_bounds/len(df):.2f}%)")
        
        self.results['step2'] = stats
        
        return df
    
    def step3_create_attack_labels(self, df: pd.DataFrame, inject_synthetic: bool = True) -> pd.DataFrame:
        """Create attack labels based on anomaly detection + synthetic injection."""
        self.log("STEP 3: Creating attack labels...", "START")
        
        df = df.copy()
        rng = np.random.default_rng(42)
        
        # Initialize attack label
        df['attack_label'] = 0
        df['attack_type'] = 'normal'
        
        # First detect natural anomalies
        # Speed anomalies: > 45 m/s (100+ mph)
        speed_threshold = 45  # m/s
        speed_anomaly = df['speed'] > speed_threshold
        df.loc[speed_anomaly, 'attack_label'] = 1
        df.loc[speed_anomaly, 'attack_type'] = 'speed_anomaly'
        
        # Position anomalies: out of Wyoming bounds
        if 'latitude' in df.columns and 'longitude' in df.columns:
            position_anomaly = ((df['latitude'] < 40.5) | (df['latitude'] > 45.5) |
                              (df['longitude'] < -111.5) | (df['longitude'] > -103.5))
            df.loc[position_anomaly, 'attack_label'] = 1
            df.loc[position_anomaly, 'attack_type'] = 'position_anomaly'
        
        natural_attacks = df['attack_label'].sum()
        self.log(f"Natural anomalies detected: {natural_attacks}")
        
        # Inject synthetic attacks if needed for ML testing
        if inject_synthetic and df['attack_label'].mean() < 0.10:
            self.log("Injecting synthetic attacks for ML testing...", "START")
            
            n_to_inject = int(len(df) * 0.15)  # 15% attack ratio
            normal_indices = df[df['attack_label'] == 0].index.tolist()
            attack_indices = rng.choice(normal_indices, size=min(n_to_inject, len(normal_indices)), replace=False)
            
            # Split among attack types
            attack_splits = np.array_split(attack_indices, 3)
            
            # Speed attacks: multiply speed by 2-5x
            speed_idx = attack_splits[0]
            df.loc[speed_idx, 'speed'] = df.loc[speed_idx, 'speed'] * rng.uniform(2, 5, len(speed_idx))
            df.loc[speed_idx, 'attack_label'] = 1
            df.loc[speed_idx, 'attack_type'] = 'synthetic_speed'
            
            # Position attacks: shift latitude/longitude significantly
            pos_idx = attack_splits[1]
            df.loc[pos_idx, 'latitude'] += rng.uniform(0.5, 2, len(pos_idx)) * rng.choice([-1, 1], len(pos_idx))
            df.loc[pos_idx, 'attack_label'] = 1
            df.loc[pos_idx, 'attack_type'] = 'synthetic_position'
            
            # Heading attacks: random heading changes
            heading_idx = attack_splits[2]
            if 'heading' in df.columns:
                df.loc[heading_idx, 'heading'] = rng.uniform(0, 360, len(heading_idx))
            df.loc[heading_idx, 'attack_label'] = 1
            df.loc[heading_idx, 'attack_type'] = 'synthetic_heading'
            
            self.log(f"Injected {n_to_inject} synthetic attacks")
        
        attack_counts = df['attack_type'].value_counts()
        self.log("Attack distribution:", "DATA")
        for attack_type, count in attack_counts.items():
            pct = 100 * count / len(df)
            self.log(f"  - {attack_type}: {count} ({pct:.1f}%)")
        
        self.results['step3'] = {
            'total': len(df),
            'natural_attacks': int(natural_attacks),
            'attack_count': int(df['attack_label'].sum()),
            'attack_ratio': float(df['attack_label'].mean()),
            'distribution': attack_counts.to_dict()
        }
        
        return df
    
    def step4_cache_data(self, df: pd.DataFrame) -> None:
        """Cache the processed data."""
        self.log("STEP 4: Caching data...", "START")
        
        cache = CacheManager(
            cache_dir=self.cache_dir,
            max_size_gb=2.0,
            verify_checksums=True
        )
        
        # Save as wyomingsample source
        cache.save_processed(df, "wyomingsample", "BSM", date(2020, 10, 6))
        
        entry = cache.get_entry("wyomingsample", "BSM", date(2020, 10, 6))
        stats = cache.get_stats()
        
        self.log(f"Cached {entry.row_count} rows ({stats['total_size_gb']:.4f}GB)")
        
        self.results['step4'] = {
            'cached_rows': entry.row_count,
            'cache_size_gb': stats['total_size_gb']
        }
    
    def step5_feature_engineering(self, df: pd.DataFrame) -> tuple:
        """Create features for ML."""
        self.log("STEP 5: Feature engineering...", "START")
        
        df = df.copy()
        
        # Basic features
        df['speed_zscore'] = (df['speed'] - df['speed'].mean()) / (df['speed'].std() + 1e-6)
        df['lat_zscore'] = (df['latitude'] - df['latitude'].mean()) / (df['latitude'].std() + 1e-6)
        df['lon_zscore'] = (df['longitude'] - df['longitude'].mean()) / (df['longitude'].std() + 1e-6)
        
        if 'heading' in df.columns:
            df['heading_sin'] = np.sin(np.radians(df['heading']))
            df['heading_cos'] = np.cos(np.radians(df['heading']))
        
        if 'elevation' in df.columns:
            df['elevation_norm'] = (df['elevation'] - df['elevation'].min()) / (df['elevation'].max() - df['elevation'].min() + 1e-6)
        
        # Anomaly indicators
        df['is_speed_outlier'] = (np.abs(df['speed_zscore']) > 3).astype(int)
        df['is_position_outlier'] = ((np.abs(df['lat_zscore']) > 3) | (np.abs(df['lon_zscore']) > 3)).astype(int)
        
        # Feature columns
        feature_cols = [
            'latitude', 'longitude', 'speed',
            'speed_zscore', 'lat_zscore', 'lon_zscore',
            'is_speed_outlier', 'is_position_outlier'
        ]
        
        if 'heading' in df.columns:
            feature_cols.extend(['heading', 'heading_sin', 'heading_cos'])
        if 'elevation' in df.columns:
            feature_cols.extend(['elevation', 'elevation_norm'])
        
        # Filter to available features
        feature_cols = [c for c in feature_cols if c in df.columns]
        
        X = df[feature_cols].fillna(0).values
        y = df['attack_label'].values
        
        # Scale
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        self.log(f"Created {len(feature_cols)} features for {len(X)} samples")
        self.log(f"Class balance: {np.bincount(y.astype(int))}")
        
        self.results['step5'] = {
            'n_features': len(feature_cols),
            'feature_names': feature_cols,
            'n_samples': len(X),
            'class_counts': np.bincount(y.astype(int)).tolist()
        }
        
        return X_scaled, y, feature_cols
    
    def step6_train_and_evaluate(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """Train and evaluate ML models."""
        self.log("STEP 6: Training ML models...", "START")
        
        # Check class balance
        attack_ratio = y.mean()
        if attack_ratio < 0.01:
            self.log(f"Warning: Very imbalanced data ({attack_ratio:.3%} attacks)", "WARN")
        
        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        self.log(f"Train: {len(X_train)}, Test: {len(X_test)}")
        
        results = {}
        
        # Random Forest
        self.log("Training Random Forest...", "START")
        rf = RandomForestClassifier(n_estimators=100, max_depth=15, n_jobs=-1, random_state=42)
        
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        rf_cv = cross_val_score(rf, X_train, y_train, cv=cv, scoring='f1')
        
        rf.fit(X_train, y_train)
        rf_pred = rf.predict(X_test)
        rf_proba = rf.predict_proba(X_test)[:, 1] if len(rf.classes_) == 2 else rf.predict_proba(X_test)[:, 0]
        
        results['random_forest'] = {
            'cv_f1': float(rf_cv.mean()),
            'cv_f1_std': float(rf_cv.std()),
            'accuracy': float(accuracy_score(y_test, rf_pred)),
            'precision': float(precision_score(y_test, rf_pred, zero_division=0)),
            'recall': float(recall_score(y_test, rf_pred, zero_division=0)),
            'f1': float(f1_score(y_test, rf_pred, zero_division=0)),
            'roc_auc': float(roc_auc_score(y_test, rf_proba)) if len(np.unique(y_test)) > 1 else 0.0
        }
        
        self.log(f"Random Forest: F1={results['random_forest']['f1']:.3f}, AUC={results['random_forest']['roc_auc']:.3f}")
        
        # Gradient Boosting
        self.log("Training Gradient Boosting...", "START")
        gb = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42)
        
        gb_cv = cross_val_score(gb, X_train, y_train, cv=cv, scoring='f1')
        
        gb.fit(X_train, y_train)
        gb_pred = gb.predict(X_test)
        gb_proba = gb.predict_proba(X_test)[:, 1] if len(gb.classes_) == 2 else gb.predict_proba(X_test)[:, 0]
        
        results['gradient_boosting'] = {
            'cv_f1': float(gb_cv.mean()),
            'cv_f1_std': float(gb_cv.std()),
            'accuracy': float(accuracy_score(y_test, gb_pred)),
            'precision': float(precision_score(y_test, gb_pred, zero_division=0)),
            'recall': float(recall_score(y_test, gb_pred, zero_division=0)),
            'f1': float(f1_score(y_test, gb_pred, zero_division=0)),
            'roc_auc': float(roc_auc_score(y_test, gb_proba)) if len(np.unique(y_test)) > 1 else 0.0
        }
        
        self.log(f"Gradient Boosting: F1={results['gradient_boosting']['f1']:.3f}, AUC={results['gradient_boosting']['roc_auc']:.3f}")
        
        # Feature importance
        importance = dict(zip(self.results['step5']['feature_names'], rf.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        self.log("Top 5 features:", "DATA")
        for feat, imp in sorted_imp[:5]:
            self.log(f"  - {feat}: {imp:.4f}")
        
        results['feature_importance'] = {k: float(v) for k, v in sorted_imp}
        
        self.results['step6'] = results
        
        return results
    
    def run(self) -> Dict:
        """Run the full pipeline."""
        print("\n" + "="*70)
        print("  Wyoming Sample Data - Full E2E Pipeline Test")
        print("  Source: wyomingsample")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        # Run pipeline
        df = self.step1_load_data()
        print()
        
        df = self.step2_analyze_data(df)
        print()
        
        df = self.step3_create_attack_labels(df)
        print()
        
        self.step4_cache_data(df)
        print()
        
        X, y, features = self.step5_feature_engineering(df)
        print()
        
        model_results = self.step6_train_and_evaluate(X, y)
        print()
        
        elapsed = time.time() - start_time
        
        # Summary
        best_model = max(model_results.keys(), key=lambda k: model_results[k].get('f1', 0) if isinstance(model_results[k], dict) else 0)
        best_f1 = model_results[best_model]['f1'] if isinstance(model_results[best_model], dict) else 0
        
        print("="*70)
        print("  PIPELINE SUMMARY")
        print("="*70)
        print(f"\n  Total time: {elapsed:.1f}s")
        print(f"  Data: {self.results['step1']['after_cleaning']} records")
        print(f"  Attack ratio: {100*self.results['step3']['attack_ratio']:.2f}%")
        print(f"  Features: {self.results['step5']['n_features']}")
        print(f"  Best model: {best_model}")
        print(f"  Best F1: {best_f1:.3f}")
        print("\n" + "="*70)
        
        self.results['summary'] = {
            'elapsed_seconds': elapsed,
            'success': True,
            'best_model': best_model,
            'best_f1': best_f1
        }
        
        return self.results


def main():
    """Main entry point."""
    data_path = Path(__file__).parent.parent / "data" / "wyoming100M.csv"
    
    if not data_path.exists():
        print(f"ERROR: Data file not found: {data_path}")
        print("Please download the Wyoming sample data first.")
        return 1
    
    print(f"Using data file: {data_path}")
    print(f"File size: {data_path.stat().st_size / 1024 / 1024:.1f}MB")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        pipeline = WyomingSamplePipeline(data_path, Path(tmpdir) / "cache")
        results = pipeline.run()
        
        # Verify
        assert results['summary']['success'], "Pipeline failed!"
        
        print("\n✅ Wyoming sample E2E pipeline completed successfully!")
        
        return 0


if __name__ == "__main__":
    sys.exit(main())
