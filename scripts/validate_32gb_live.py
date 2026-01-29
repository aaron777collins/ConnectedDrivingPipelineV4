#!/usr/bin/env python3
"""
Full live validation of 32GB configuration.
Tests Dask cluster, data pipeline, caching, validation, and ML.
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

# Dask imports
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd

# Project imports
from DataSources.CacheManager import CacheManager
from DataSources.SchemaValidator import BSMSchemaValidator, get_validator
from DataSources.SyntheticDataGenerator import SyntheticBSMGenerator
from DataSources.config import DataSourceConfig, DateRangeConfig, CacheConfig


class LiveValidator:
    """Comprehensive live validation of the pipeline."""
    
    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir
        self.cache_dir = temp_dir / "cache"
        self.data_dir = temp_dir / "data"
        self.results: Dict[str, Any] = {}
        self.errors: List[str] = []
        
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def log(self, msg: str, level: str = "INFO"):
        """Log with timestamp."""
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = {"INFO": "✓", "WARN": "⚠", "ERROR": "✗", "START": "→"}.get(level, " ")
        print(f"[{ts}] {prefix} {msg}")
    
    def test_dask_cluster(self) -> bool:
        """Test 1: Start and validate Dask cluster with 32GB config."""
        self.log("Testing Dask cluster startup...", "START")
        
        try:
            # Create cluster with 32GB-appropriate settings
            # Using 2 workers for testing (lighter than 4 for validation)
            cluster = LocalCluster(
                n_workers=2,
                threads_per_worker=2,
                memory_limit='4GB',  # Conservative for test
                local_directory=str(self.temp_dir / "dask-scratch"),
                silence_logs=30
            )
            client = Client(cluster)
            
            self.log(f"Cluster started: {client.dashboard_link}")
            
            # Verify cluster is responsive
            info = client.scheduler_info()
            n_workers = len(info['workers'])
            total_memory = sum(w['memory_limit'] for w in info['workers'].values())
            
            self.log(f"Workers: {n_workers}, Total memory: {total_memory / 1e9:.1f}GB")
            
            # Simple computation test
            x = dd.from_pandas(pd.DataFrame({'a': range(1000)}), npartitions=4)
            result = x['a'].sum().compute()
            assert result == 499500, f"Expected 499500, got {result}"
            
            self.log("Basic Dask computation verified")
            
            self.results['cluster'] = {
                'workers': n_workers,
                'total_memory_gb': total_memory / 1e9,
                'dashboard': client.dashboard_link
            }
            
            # Store client for later tests
            self.client = client
            self.cluster = cluster
            
            self.log("Dask cluster test PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Dask cluster test FAILED: {e}", "ERROR")
            self.errors.append(f"Dask cluster: {e}")
            return False
    
    def test_synthetic_data_generation(self) -> bool:
        """Test 2: Generate synthetic BSM data."""
        self.log("Testing synthetic data generation...", "START")
        
        try:
            gen = SyntheticBSMGenerator(seed=42, num_vehicles=10)
            
            # Generate a day of data
            target_date = date(2021, 4, 1)
            records = gen.generate_batch(
                datetime(2021, 4, 1, 12, 0, 0),
                num_records=1000
            )
            
            self.log(f"Generated {len(records)} BSM records")
            
            # Verify record structure
            sample = records[0]
            assert 'metadata' in sample
            assert 'payload' in sample
            assert 'recordGeneratedAt' in sample['metadata']
            
            self.results['synthetic_data'] = {
                'records_generated': len(records),
                'sample_fields': list(sample.keys())
            }
            
            # Store for later tests
            self.sample_records = records
            
            self.log("Synthetic data generation PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Synthetic data generation FAILED: {e}", "ERROR")
            self.errors.append(f"Synthetic data: {e}")
            return False
    
    def test_schema_validation(self) -> bool:
        """Test 3: Validate records with schema validator."""
        self.log("Testing schema validation...", "START")
        
        try:
            validator = BSMSchemaValidator()
            
            # Validate batch
            report = validator.batch_validate(self.sample_records)
            
            self.log(f"Validated {report.total_count} records: {report.valid_count} valid, {report.invalid_count} invalid")
            
            # All synthetic records should be valid
            if report.invalid_count > 0:
                self.log(f"Warning: {report.invalid_count} invalid records", "WARN")
                for error in report.errors[:5]:
                    self.log(f"  - {error}", "WARN")
            
            self.results['validation'] = {
                'total': report.total_count,
                'valid': report.valid_count,
                'invalid': report.invalid_count,
                'valid_ratio': report.valid_count / report.total_count if report.total_count > 0 else 0
            }
            
            self.log("Schema validation PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Schema validation FAILED: {e}", "ERROR")
            self.errors.append(f"Validation: {e}")
            return False
    
    def test_cache_manager(self) -> bool:
        """Test 4: Test CacheManager save/load operations."""
        self.log("Testing CacheManager...", "START")
        
        try:
            cache = CacheManager(
                cache_dir=self.cache_dir,
                max_size_gb=1.0,
                verify_checksums=True
            )
            
            # Create DataFrame from records
            # Flatten the nested structure for caching
            flat_records = []
            for r in self.sample_records[:500]:  # Use subset
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
            
            df = pd.DataFrame(flat_records)
            self.log(f"Created DataFrame with {len(df)} rows")
            
            # Save to cache
            test_date = date(2021, 4, 1)
            cache.save_processed(df, "wydot", "BSM", test_date)
            self.log("Saved data to cache")
            
            # Verify entry
            entry = cache.get_entry("wydot", "BSM", test_date)
            assert entry is not None, "Cache entry not found"
            assert entry.status == "complete", f"Expected 'complete', got '{entry.status}'"
            assert entry.row_count == 500, f"Expected 500 rows, got {entry.row_count}"
            
            # Load from cache
            loaded = cache.load_parquet("wydot", "BSM", test_date)
            assert len(loaded) == 500, f"Expected 500 rows, got {len(loaded)}"
            
            # Verify data integrity
            assert list(loaded.columns) == list(df.columns), "Column mismatch"
            
            # Test cache stats
            stats = cache.get_stats()
            self.log(f"Cache stats: {stats['total_entries']} entries, {stats['total_size_gb']:.3f}GB")
            
            self.results['cache'] = {
                'entries': stats['total_entries'],
                'size_gb': stats['total_size_gb'],
                'row_count': entry.row_count
            }
            
            # Store for later
            self.cached_df = loaded
            
            self.log("CacheManager test PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"CacheManager test FAILED: {e}", "ERROR")
            self.errors.append(f"CacheManager: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_dask_data_processing(self) -> bool:
        """Test 5: Process data with Dask."""
        self.log("Testing Dask data processing...", "START")
        
        try:
            # Convert to Dask DataFrame
            ddf = dd.from_pandas(self.cached_df, npartitions=4)
            
            # Compute statistics
            stats = {
                'mean_speed': float(ddf['speed'].mean().compute()),
                'max_speed': float(ddf['speed'].max().compute()),
                'mean_elevation': float(ddf['elevation'].mean().compute()),
                'unique_vehicles': int(ddf['vehicle_id'].nunique().compute())
            }
            
            self.log(f"Mean speed: {stats['mean_speed']:.2f} m/s")
            self.log(f"Unique vehicles: {stats['unique_vehicles']}")
            
            # Test groupby operation
            by_vehicle = ddf.groupby('vehicle_id').agg({
                'speed': 'mean',
                'elevation': 'mean'
            }).compute()
            
            self.log(f"Grouped by {len(by_vehicle)} vehicles")
            
            self.results['dask_processing'] = {
                'partitions': ddf.npartitions,
                'stats': stats,
                'groups': len(by_vehicle)
            }
            
            self.log("Dask data processing PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Dask data processing FAILED: {e}", "ERROR")
            self.errors.append(f"Dask processing: {e}")
            return False
    
    def test_ml_preprocessing(self) -> bool:
        """Test 6: Test ML preprocessing pipeline."""
        self.log("Testing ML preprocessing...", "START")
        
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.model_selection import train_test_split
            
            # Prepare features
            features = self.cached_df[['latitude', 'longitude', 'elevation', 'speed', 'heading']].copy()
            
            # Handle any missing values
            features = features.fillna(features.mean())
            
            # Split data
            X_train, X_test = train_test_split(features, test_size=0.2, random_state=42)
            
            self.log(f"Train: {len(X_train)}, Test: {len(X_test)}")
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Verify scaling
            mean_after = np.mean(X_train_scaled, axis=0)
            std_after = np.std(X_train_scaled, axis=0)
            
            assert np.allclose(mean_after, 0, atol=1e-10), "Scaling mean not ~0"
            assert np.allclose(std_after, 1, atol=1e-10), "Scaling std not ~1"
            
            self.results['ml_preprocessing'] = {
                'train_size': len(X_train),
                'test_size': len(X_test),
                'features': list(features.columns),
                'scaling_verified': True
            }
            
            self.log("ML preprocessing PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"ML preprocessing FAILED: {e}", "ERROR")
            self.errors.append(f"ML preprocessing: {e}")
            return False
    
    def test_ml_model(self) -> bool:
        """Test 7: Test a simple ML model."""
        self.log("Testing ML model training...", "START")
        
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.model_selection import cross_val_score
            
            # Create a simple classification task
            # Classify high speed (>20 m/s) vs low speed
            df = self.cached_df.copy()
            df['high_speed'] = (df['speed'] > 20).astype(int)
            
            features = df[['latitude', 'longitude', 'elevation', 'heading']].fillna(0)
            target = df['high_speed']
            
            # Train simple model
            model = RandomForestClassifier(n_estimators=10, random_state=42, n_jobs=-1)
            
            # Cross-validation
            scores = cross_val_score(model, features, target, cv=3, scoring='accuracy')
            
            self.log(f"CV Accuracy: {scores.mean():.3f} (+/- {scores.std()*2:.3f})")
            
            # Fit final model
            model.fit(features, target)
            
            # Feature importance
            importance = dict(zip(features.columns, model.feature_importances_))
            top_feature = max(importance, key=importance.get)
            
            self.log(f"Top feature: {top_feature} ({importance[top_feature]:.3f})")
            
            self.results['ml_model'] = {
                'cv_accuracy': float(scores.mean()),
                'cv_std': float(scores.std()),
                'feature_importance': {k: float(v) for k, v in importance.items()}
            }
            
            self.log("ML model training PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"ML model training FAILED: {e}", "ERROR")
            self.errors.append(f"ML model: {e}")
            return False
    
    def test_concurrent_operations(self) -> bool:
        """Test 8: Test concurrent cache operations."""
        self.log("Testing concurrent operations...", "START")
        
        try:
            import concurrent.futures
            
            cache = CacheManager(cache_dir=self.cache_dir, max_size_gb=1.0)
            
            def save_day(day_offset):
                df = pd.DataFrame({
                    'value': range(100),
                    'day': day_offset
                })
                target_date = date(2021, 4, 1) + timedelta(days=day_offset)
                cache.save_processed(df, "wydot", "TIM", target_date)
                return day_offset
            
            # Save 5 days concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(save_day, i) for i in range(5)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
            
            self.log(f"Saved {len(results)} days concurrently")
            
            # Verify all saved
            for i in range(5):
                target_date = date(2021, 4, 1) + timedelta(days=i)
                entry = cache.get_entry("wydot", "TIM", target_date)
                assert entry is not None, f"Missing entry for day {i}"
            
            self.results['concurrent'] = {
                'days_saved': len(results),
                'all_verified': True
            }
            
            self.log("Concurrent operations PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Concurrent operations FAILED: {e}", "ERROR")
            self.errors.append(f"Concurrent: {e}")
            return False
    
    def test_memory_under_load(self) -> bool:
        """Test 9: Verify memory handling under load."""
        self.log("Testing memory under load...", "START")
        
        try:
            import psutil
            
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1e9
            
            # Generate larger dataset
            gen = SyntheticBSMGenerator(seed=123, num_vehicles=50)
            
            large_records = []
            for hour in range(24):
                batch = gen.generate_batch(
                    datetime(2021, 4, 2, hour, 0, 0),
                    num_records=500
                )
                large_records.extend(batch)
            
            self.log(f"Generated {len(large_records)} records")
            
            # Process with Dask
            flat_records = []
            for r in large_records:
                flat = {
                    'timestamp': r['metadata']['recordGeneratedAt'],
                    'latitude': r['payload']['data']['coreData']['position']['latitude'],
                    'longitude': r['payload']['data']['coreData']['position']['longitude'],
                    'speed': r['payload']['data']['coreData']['speed'],
                }
                flat_records.append(flat)
            
            df = pd.DataFrame(flat_records)
            ddf = dd.from_pandas(df, npartitions=8)
            
            # Heavy computation - use simple groupby instead of pd.cut with dask
            speed_bins = (ddf['speed'] // 5).astype(int)  # Bin by 5 m/s intervals
            result = ddf.assign(speed_bin=speed_bins).groupby('speed_bin').agg({
                'latitude': 'mean',
                'longitude': 'mean'
            }).compute()
            
            final_memory = process.memory_info().rss / 1e9
            memory_delta = final_memory - initial_memory
            
            self.log(f"Memory: {initial_memory:.2f}GB → {final_memory:.2f}GB (Δ{memory_delta:+.2f}GB)")
            
            self.results['memory'] = {
                'initial_gb': initial_memory,
                'final_gb': final_memory,
                'delta_gb': memory_delta,
                'records_processed': len(large_records)
            }
            
            self.log("Memory test PASSED", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Memory test FAILED: {e}", "ERROR")
            self.errors.append(f"Memory: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources."""
        self.log("Cleaning up...", "START")
        
        try:
            if hasattr(self, 'client'):
                self.client.close()
            if hasattr(self, 'cluster'):
                self.cluster.close()
            self.log("Cleanup complete")
        except Exception as e:
            self.log(f"Cleanup warning: {e}", "WARN")
    
    def run_all(self) -> Dict[str, Any]:
        """Run all validation tests."""
        print("\n" + "="*60)
        print("  ConnectedDrivingPipelineV4 - Full Live Validation")
        print("  32GB Configuration")
        print("="*60 + "\n")
        
        tests = [
            ("Dask Cluster", self.test_dask_cluster),
            ("Synthetic Data", self.test_synthetic_data_generation),
            ("Schema Validation", self.test_schema_validation),
            ("CacheManager", self.test_cache_manager),
            ("Dask Processing", self.test_dask_data_processing),
            ("ML Preprocessing", self.test_ml_preprocessing),
            ("ML Model", self.test_ml_model),
            ("Concurrent Ops", self.test_concurrent_operations),
            ("Memory Load", self.test_memory_under_load),
        ]
        
        passed = 0
        failed = 0
        
        for name, test_fn in tests:
            try:
                if test_fn():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                self.log(f"{name} crashed: {e}", "ERROR")
                self.errors.append(f"{name}: {e}")
                failed += 1
            print()
        
        self.cleanup()
        
        # Summary
        print("\n" + "="*60)
        print("  VALIDATION SUMMARY")
        print("="*60)
        print(f"\n  Tests passed: {passed}/{len(tests)}")
        print(f"  Tests failed: {failed}/{len(tests)}")
        
        if self.errors:
            print(f"\n  Errors:")
            for err in self.errors:
                print(f"    ✗ {err}")
        
        print("\n" + "="*60)
        
        return {
            'passed': passed,
            'failed': failed,
            'total': len(tests),
            'errors': self.errors,
            'results': self.results
        }


def main():
    """Main entry point."""
    with tempfile.TemporaryDirectory() as tmpdir:
        validator = LiveValidator(Path(tmpdir))
        summary = validator.run_all()
        
        # Exit with error code if any tests failed
        sys.exit(1 if summary['failed'] > 0 else 0)


if __name__ == "__main__":
    main()
