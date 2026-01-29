"""
End-to-end pipeline test for DataSources module.

Tests the full flow:
1. Config validation
2. Schema validation of BSM records
3. CacheManager operations (save, load, manifest)
4. DataFrame creation
"""

import json
import shutil
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import sys
sys.path.insert(0, '/home/ubuntu/ConnectedDrivingPipelineV4')

from DataSources.config import (
    DataSourceConfig,
    generate_cache_key,
    validate_source_and_type,
)
from DataSources.CacheManager import CacheManager
from DataSources.SchemaValidator import BSMSchemaValidator, ValidationReport


# Path to sample data
SAMPLE_DATA_PATH = Path('/home/ubuntu/ConnectedDrivingPipelineV4/Test/test_data/sample_bsm.ndjson')


def load_sample_records():
    """Load sample BSM records from NDJSON file."""
    records = []
    with open(SAMPLE_DATA_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def test_config_validation():
    """Test that config validation works correctly."""
    print("=" * 60)
    print("TEST: Config Validation")
    print("=" * 60)
    
    # Valid config
    config = DataSourceConfig(
        source="wydot",
        message_type="BSM",
        date_range={"start_date": "2021-04-01", "end_date": "2021-04-01"}
    )
    print(f"  ✓ Valid config created: {config.source}/{config.message_type}")
    print(f"    Hash: {config.compute_config_hash()}")
    
    # Test source validation function
    validate_source_and_type("wydot", "BSM")
    print("  ✓ Source/type validation passed")
    
    # Test cache key generation
    key = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
    assert key == "wydot/BSM/2021/04/01", f"Wrong key: {key}"
    print(f"  ✓ Cache key generated: {key}")
    
    # Test invalid config rejection
    try:
        validate_source_and_type("nycdot", "BSM")
        print("  ✗ Should have rejected BSM for nycdot!")
        return False
    except ValueError:
        print("  ✓ Invalid source/type correctly rejected")
    
    print()
    return True


def test_schema_validation():
    """Test BSM schema validation with sample data."""
    print("=" * 60)
    print("TEST: Schema Validation")
    print("=" * 60)
    
    records = load_sample_records()
    print(f"  Loaded {len(records)} sample records")
    
    validator = BSMSchemaValidator()
    print(f"  Validator has {len(validator.REQUIRED_FIELDS)} required fields")
    
    # Validate each record
    all_valid = True
    for i, record in enumerate(records):
        is_valid, errors = validator.validate(record)
        if is_valid:
            print(f"  ✓ Record {i+1}: Valid")
        else:
            print(f"  ✗ Record {i+1}: Invalid - {errors}")
            all_valid = False
    
    # Batch validation
    report = validator.batch_validate(records)
    print(f"\n  Batch validation report:")
    print(f"    Valid: {report.valid_count}")
    print(f"    Invalid: {report.invalid_count}")
    print(f"    Warnings: {len(report.warnings)}")
    
    if report.warnings:
        print(f"    Sample warnings:")
        for w in report.warnings[:3]:
            print(f"      - {w}")
    
    # Test flattening
    print("\n  Testing record flattening:")
    flat = validator.flatten_record(records[0])
    print(f"    latitude: {flat.get('latitude')}")
    print(f"    longitude: {flat.get('longitude')}")
    print(f"    speed: {flat.get('speed')}")
    print(f"    heading: {flat.get('heading')}")
    
    print()
    return all_valid


def test_cache_manager():
    """Test CacheManager operations."""
    print("=" * 60)
    print("TEST: CacheManager Operations")
    print("=" * 60)
    
    # Create temp directory for cache
    cache_dir = Path(tempfile.mkdtemp(prefix="wydot_cache_test_"))
    print(f"  Using temp cache: {cache_dir}")
    
    try:
        # Initialize cache manager
        cache = CacheManager(cache_dir=cache_dir, max_size_gb=1.0)
        print("  ✓ CacheManager initialized")
        
        # Load and flatten sample records
        records = load_sample_records()
        validator = BSMSchemaValidator()
        flat_records = [validator.flatten_record(r) for r in records]
        
        # Create DataFrame
        df = pd.DataFrame(flat_records)
        print(f"  ✓ Created DataFrame with {len(df)} rows, {len(df.columns)} columns")
        print(f"    Columns: {list(df.columns)[:5]}...")
        
        # Save to cache
        test_date = date(2021, 4, 1)
        saved_path = cache.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=test_date
        )
        print(f"  ✓ Saved to cache: {saved_path}")
        
        # Check manifest
        manifest = cache._load_manifest()
        key = generate_cache_key("wydot", "BSM", test_date)
        assert key in manifest.get('entries', {}), "Key not in manifest"
        entry = manifest['entries'][key]
        print(f"  ✓ Manifest entry created:")
        print(f"    Status: {entry.get('status')}")
        print(f"    Rows: {entry.get('row_count')}")
        print(f"    Checksum: {entry.get('checksum_sha256', 'N/A')[:16]}...")
        
        # Test cache hit
        cached_dates = cache.get_cached_dates("wydot", "BSM")
        assert test_date in cached_dates, "Date not in cached dates"
        print(f"  ✓ Cache hit detected: {test_date}")
        
        # Test missing dates
        missing = cache.get_missing_dates(
            "wydot", "BSM",
            date(2021, 4, 1), date(2021, 4, 3)
        )
        assert date(2021, 4, 2) in missing, "April 2 should be missing"
        assert date(2021, 4, 3) in missing, "April 3 should be missing"
        print(f"  ✓ Missing dates detected: {missing}")
        
        # Load from cache
        loaded_df = cache.load_date_range(
            "wydot", "BSM",
            date(2021, 4, 1), date(2021, 4, 1)
        )
        assert len(loaded_df) == len(df), f"Row count mismatch: {len(loaded_df)} vs {len(df)}"
        print(f"  ✓ Loaded from cache: {len(loaded_df)} rows")
        
        # Test no_data marking
        cache.mark_no_data("wydot", "BSM", date(2021, 4, 5))
        manifest = cache._load_manifest()
        no_data_key = generate_cache_key("wydot", "BSM", date(2021, 4, 5))
        assert manifest['entries'].get(no_data_key, {}).get('status') == 'no_data'
        print(f"  ✓ No-data marking works")
        
        # Test cache clear
        cache.clear_cache(source="wydot", msg_type="BSM")
        cached_dates = cache.get_cached_dates("wydot", "BSM")
        # Note: no_data entries might still be in manifest
        print(f"  ✓ Cache cleared")
        
        print()
        return True
        
    finally:
        # Cleanup
        shutil.rmtree(cache_dir, ignore_errors=True)
        print(f"  Cleaned up temp cache")


def test_full_pipeline():
    """Test the full data pipeline with sample data."""
    print("=" * 60)
    print("TEST: Full Pipeline Integration")
    print("=" * 60)
    
    cache_dir = Path(tempfile.mkdtemp(prefix="wydot_pipeline_test_"))
    print(f"  Cache directory: {cache_dir}")
    
    try:
        # 1. Create config
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01", "end_date": "2021-04-01"},
            cache={"directory": str(cache_dir)}
        )
        print(f"  1. Config created for {config.source}/{config.message_type}")
        
        # 2. Load and validate records
        records = load_sample_records()
        validator = BSMSchemaValidator()
        report = validator.batch_validate(records)
        print(f"  2. Validated {report.valid_count}/{report.total_count} records")
        
        # 3. Flatten and create DataFrame
        flat_records = [validator.flatten_record(r) for r in records]
        df = pd.DataFrame(flat_records)
        print(f"  3. Created DataFrame: {df.shape}")
        
        # 4. Save to cache
        cache = CacheManager(cache_dir=cache_dir)
        test_date = date(2021, 4, 1)
        cache.save_processed(df, "wydot", "BSM", test_date)
        print(f"  4. Saved to cache: {generate_cache_key('wydot', 'BSM', test_date)}")
        
        # 5. Verify cache integrity
        manifest = cache._load_manifest()
        key = generate_cache_key("wydot", "BSM", test_date)
        entry = manifest['entries'][key]
        assert entry['status'] == 'complete'
        print(f"  5. Cache integrity verified: status={entry['status']}")
        
        # 6. Load from cache
        loaded = cache.load_date_range("wydot", "BSM", test_date, test_date)
        assert len(loaded) == len(df)
        print(f"  6. Loaded from cache: {len(loaded)} rows")
        
        # 7. Verify data (handle both Dask and pandas DataFrames)
        if hasattr(loaded, 'compute'):
            loaded = loaded.compute()  # Convert Dask to pandas
        lat = loaded['latitude'].values[0]
        lon = loaded['longitude'].values[0]
        assert abs(lat - 42.850001) < 0.0001
        assert abs(lon - (-106.320001)) < 0.0001
        print(f"  7. Data verification passed!")
        print(f"     Sample: lat={lat:.6f}, lon={lon:.6f}")
        
        print("\n  ✅ FULL PIPELINE TEST PASSED!")
        print()
        return True
        
    finally:
        shutil.rmtree(cache_dir, ignore_errors=True)


def run_all_tests():
    """Run all tests and report results."""
    print("\n" + "=" * 60)
    print("DATASOURCES END-TO-END PIPELINE TESTS")
    print("=" * 60 + "\n")
    
    results = {}
    
    results['config'] = test_config_validation()
    results['schema'] = test_schema_validation()
    results['cache'] = test_cache_manager()
    results['pipeline'] = test_full_pipeline()
    
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️ SOME TESTS FAILED")
    
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
