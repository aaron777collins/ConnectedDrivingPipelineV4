#!/usr/bin/env python3
"""
End-to-End Caching Validation Test

This script validates:
1. Raw data is fetched once and cached permanently
2. Different attack configurations get separate caches
3. Same attack config reuses cache (no re-computation)
4. Different attack percentages = different cache entries
5. Different attack types = different cache entries
6. Different seeds = different cache entries
"""

import sys
import tempfile
import time
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np

from DataSources.CacheManager import CacheManager
from DataSources.ProcessedDataManager import ProcessedDataManager, AttackConfig, compute_config_hash
from DataSources.SyntheticDataGenerator import SyntheticBSMGenerator
from DataSources.config import DataSourceConfig, DateRangeConfig


class E2ECacheValidator:
    """Validates end-to-end caching behavior."""
    
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.results = []
        self.errors = []
        
    def log(self, msg: str, status: str = "INFO"):
        symbols = {"PASS": "✓", "FAIL": "✗", "INFO": "→", "WARN": "⚠"}
        print(f"[{symbols.get(status, ' ')}] {msg}")
        self.results.append((status, msg))
        if status == "FAIL":
            self.errors.append(msg)
    
    def generate_test_data(self, n_rows: int = 1000) -> pd.DataFrame:
        """Generate synthetic BSM data for testing."""
        np.random.seed(42)
        
        base_time = pd.Timestamp('2021-04-01 00:00:00')
        
        return pd.DataFrame({
            'timestamp': [base_time + pd.Timedelta(seconds=i) for i in range(n_rows)],
            'latitude': 41.0 + np.random.randn(n_rows) * 0.01,
            'longitude': -105.0 + np.random.randn(n_rows) * 0.01,
            'speed': 50.0 + np.random.randn(n_rows) * 10,
            'heading': np.random.uniform(0, 360, n_rows),
            'elevation': 1500 + np.random.randn(n_rows) * 10,
            'vehicle_id': [f"VEH_{i % 100:04d}" for i in range(n_rows)],
        })
    
    def inject_attack(self, df: pd.DataFrame, config: AttackConfig) -> pd.DataFrame:
        """Inject attacks based on config."""
        np.random.seed(config.seed)
        
        attacked = df.copy()
        n_attack = int(len(df) * config.attack_ratio)
        attack_indices = np.random.choice(len(df), n_attack, replace=False)
        
        for attack_type in config.attack_types:
            if attack_type == 'speed':
                attacked.loc[attack_indices, 'speed'] += np.random.uniform(50, 100, n_attack)
            elif attack_type == 'position':
                attacked.loc[attack_indices, 'latitude'] += np.random.uniform(0.1, 0.5, n_attack)
                attacked.loc[attack_indices, 'longitude'] += np.random.uniform(0.1, 0.5, n_attack)
            elif attack_type == 'heading':
                attacked.loc[attack_indices, 'heading'] = np.random.uniform(0, 360, n_attack)
        
        attacked['is_attacker'] = False
        attacked.loc[attack_indices, 'is_attacker'] = True
        
        return attacked
    
    def test_raw_data_caching(self) -> bool:
        """Test 1: Raw data is cached and not re-fetched."""
        self.log("Testing raw data caching...", "INFO")
        
        cm = CacheManager(self.cache_dir / "raw", max_size_gb=1.0)
        test_date = date(2021, 4, 1)
        
        # Generate and save raw data
        raw_data = self.generate_test_data(1000)
        cm.save_processed(raw_data, "wyomingsample", "BSM", test_date)
        
        # Verify it's cached
        cached = cm.load_parquet("wyomingsample", "BSM", test_date)
        if cached is None:
            self.log("Raw data not cached!", "FAIL")
            return False
        
        if len(cached) != len(raw_data):
            self.log(f"Cached data length mismatch: {len(cached)} vs {len(raw_data)}", "FAIL")
            return False
        
        # Verify cache stats
        stats = cm.get_stats()
        if stats['total_entries'] < 1:
            self.log("Cache stats don't show entry", "FAIL")
            return False
        
        self.log(f"Raw data cached successfully ({len(cached)} rows)", "PASS")
        return True
    
    def test_different_attack_ratios(self) -> bool:
        """Test 2: Different attack ratios get different cache entries."""
        self.log("Testing different attack ratios...", "INFO")
        
        raw_data = self.generate_test_data(1000)
        test_date = date(2021, 4, 1)
        
        # Create configs with different ratios
        configs = [
            AttackConfig(attack_ratio=0.10, seed=42),
            AttackConfig(attack_ratio=0.15, seed=42),
            AttackConfig(attack_ratio=0.20, seed=42),
            AttackConfig(attack_ratio=0.30, seed=42),
        ]
        
        # Verify all have different hashes
        hashes = [c.get_hash() for c in configs]
        if len(set(hashes)) != len(hashes):
            self.log("Different ratios produced duplicate hashes!", "FAIL")
            return False
        
        # Process and cache each config
        for config in configs:
            mgr = ProcessedDataManager(self.cache_dir, config.to_dict())
            attacked = self.inject_attack(raw_data, config)
            mgr.save_processed(attacked, "wyomingsample", "BSM", test_date)
        
        # Verify each has separate cache file
        cache_files = list((self.cache_dir / "processed" / "wyomingsample" / "BSM").rglob("*.parquet"))
        if len(cache_files) < len(configs):
            self.log(f"Expected {len(configs)} cache files, found {len(cache_files)}", "FAIL")
            return False
        
        self.log(f"Different ratios: {len(configs)} configs → {len(cache_files)} cache files", "PASS")
        return True
    
    def test_different_attack_types(self) -> bool:
        """Test 3: Different attack types get different cache entries."""
        self.log("Testing different attack types...", "INFO")
        
        raw_data = self.generate_test_data(1000)
        test_date = date(2021, 4, 2)  # Different date to avoid collision
        
        configs = [
            AttackConfig(attack_ratio=0.15, attack_types=['speed'], seed=42),
            AttackConfig(attack_ratio=0.15, attack_types=['position'], seed=42),
            AttackConfig(attack_ratio=0.15, attack_types=['heading'], seed=42),
            AttackConfig(attack_ratio=0.15, attack_types=['speed', 'position'], seed=42),
            AttackConfig(attack_ratio=0.15, attack_types=['speed', 'position', 'heading'], seed=42),
        ]
        
        hashes = [c.get_hash() for c in configs]
        if len(set(hashes)) != len(hashes):
            self.log("Different attack types produced duplicate hashes!", "FAIL")
            return False
        
        for config in configs:
            mgr = ProcessedDataManager(self.cache_dir, config.to_dict())
            attacked = self.inject_attack(raw_data, config)
            mgr.save_processed(attacked, "wyomingsample", "BSM", test_date)
        
        self.log(f"Different attack types: {len(configs)} configs with unique hashes", "PASS")
        return True
    
    def test_different_seeds(self) -> bool:
        """Test 4: Different seeds get different cache entries."""
        self.log("Testing different seeds...", "INFO")
        
        raw_data = self.generate_test_data(1000)
        test_date = date(2021, 4, 3)
        
        configs = [
            AttackConfig(attack_ratio=0.15, seed=1),
            AttackConfig(attack_ratio=0.15, seed=42),
            AttackConfig(attack_ratio=0.15, seed=123),
            AttackConfig(attack_ratio=0.15, seed=999),
        ]
        
        hashes = [c.get_hash() for c in configs]
        if len(set(hashes)) != len(hashes):
            self.log("Different seeds produced duplicate hashes!", "FAIL")
            return False
        
        for config in configs:
            mgr = ProcessedDataManager(self.cache_dir, config.to_dict())
            attacked = self.inject_attack(raw_data, config)
            mgr.save_processed(attacked, "wyomingsample", "BSM", test_date)
        
        self.log(f"Different seeds: {len(configs)} configs with unique hashes", "PASS")
        return True
    
    def test_same_config_reuses_cache(self) -> bool:
        """Test 5: Same config reuses cache (no re-computation)."""
        self.log("Testing cache reuse for same config...", "INFO")
        
        raw_data = self.generate_test_data(1000)
        test_date = date(2021, 4, 4)
        config = AttackConfig(attack_ratio=0.15, seed=42)
        
        # First save
        mgr1 = ProcessedDataManager(self.cache_dir, config.to_dict())
        attacked = self.inject_attack(raw_data, config)
        path1 = mgr1.save_processed(attacked, "wyomingsample", "BSM", test_date)
        mtime1 = path1.stat().st_mtime
        
        # Second load with same config
        mgr2 = ProcessedDataManager(self.cache_dir, config.to_dict())
        
        # Verify they point to same file
        if mgr1.config_hash != mgr2.config_hash:
            self.log("Same config produced different hashes!", "FAIL")
            return False
        
        # Verify cache exists
        if not mgr2.exists("wyomingsample", "BSM", test_date):
            self.log("Cache not found for same config!", "FAIL")
            return False
        
        # Load from cache
        cached = mgr2.load_processed("wyomingsample", "BSM", test_date)
        if cached is None:
            self.log("Failed to load cached data!", "FAIL")
            return False
        
        if len(cached) != len(attacked):
            self.log("Cached data length mismatch!", "FAIL")
            return False
        
        self.log("Same config reuses cache correctly", "PASS")
        return True
    
    def test_extra_params_affect_hash(self) -> bool:
        """Test 6: Extra custom parameters affect the hash."""
        self.log("Testing extra parameters in config...", "INFO")
        
        configs = [
            AttackConfig(attack_ratio=0.15, seed=42, offset_min=10, offset_max=20),
            AttackConfig(attack_ratio=0.15, seed=42, offset_min=50, offset_max=100),
            AttackConfig(attack_ratio=0.15, seed=42, offset_min=100, offset_max=200),
            AttackConfig(attack_ratio=0.15, seed=42, model_type='random_forest'),
            AttackConfig(attack_ratio=0.15, seed=42, model_type='gradient_boost'),
        ]
        
        hashes = [c.get_hash() for c in configs]
        if len(set(hashes)) != len(hashes):
            self.log("Different extra params produced duplicate hashes!", "FAIL")
            return False
        
        self.log(f"Extra params: {len(configs)} configs with unique hashes", "PASS")
        return True
    
    def test_cache_isolation(self) -> bool:
        """Test 7: Verify complete isolation between raw and processed."""
        self.log("Testing raw/processed cache isolation...", "INFO")
        
        raw_data = self.generate_test_data(500)
        test_date = date(2021, 4, 5)
        
        # Save raw data
        raw_cm = CacheManager(self.cache_dir / "raw", max_size_gb=1.0)
        raw_cm.save_processed(raw_data, "wyomingsample", "BSM", test_date)
        
        # Save processed data with attack
        config = AttackConfig(attack_ratio=0.20, seed=42)
        proc_mgr = ProcessedDataManager(self.cache_dir, config.to_dict())
        attacked = self.inject_attack(raw_data, config)
        proc_mgr.save_processed(attacked, "wyomingsample", "BSM", test_date)
        
        # Verify raw data is unchanged
        raw_loaded = raw_cm.load_parquet("wyomingsample", "BSM", test_date)
        if 'is_attacker' in raw_loaded.columns:
            self.log("Raw data was contaminated with attack column!", "FAIL")
            return False
        
        # Verify processed data has attacks
        proc_loaded = proc_mgr.load_processed("wyomingsample", "BSM", test_date)
        if 'is_attacker' not in proc_loaded.columns:
            self.log("Processed data missing attack column!", "FAIL")
            return False
        
        attack_count = proc_loaded['is_attacker'].sum()
        expected_attacks = int(len(raw_data) * config.attack_ratio)
        if attack_count != expected_attacks:
            self.log(f"Attack count mismatch: {attack_count} vs {expected_attacks}", "FAIL")
            return False
        
        self.log(f"Cache isolation verified (raw={len(raw_loaded)}, proc={len(proc_loaded)}, attacks={attack_count})", "PASS")
        return True
    
    def run_all_tests(self) -> bool:
        """Run all validation tests."""
        print("=" * 60)
        print("End-to-End Caching Validation")
        print("=" * 60)
        
        tests = [
            self.test_raw_data_caching,
            self.test_different_attack_ratios,
            self.test_different_attack_types,
            self.test_different_seeds,
            self.test_same_config_reuses_cache,
            self.test_extra_params_affect_hash,
            self.test_cache_isolation,
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                self.log(f"Test {test.__name__} raised exception: {e}", "FAIL")
                failed += 1
        
        print("=" * 60)
        print(f"Results: {passed} passed, {failed} failed")
        
        if failed == 0:
            print("✓ ALL TESTS PASSED")
        else:
            print("✗ SOME TESTS FAILED:")
            for err in self.errors:
                print(f"  - {err}")
        
        print("=" * 60)
        return failed == 0


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        validator = E2ECacheValidator(Path(tmpdir))
        success = validator.run_all_tests()
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
