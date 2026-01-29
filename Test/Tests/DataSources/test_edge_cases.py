"""
Edge case tests for DataSources module.

Tests:
1. Empty DataFrame handling (no records)
2. Invalid JSON in NDJSON file (mixed valid/invalid lines)
3. Missing required fields in records
4. Corrupted parquet file detection and handling
5. Date range with no data (all dates marked no_data)
6. Very large row counts (test with 10000+ records)
7. Source isolation (verify wydot and nycdot data cannot mix)
"""

import json
import os
import sys
import tempfile
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, List
import hashlib

import pandas as pd
import pytest

# Add project root to path
sys.path.insert(0, '/home/ubuntu/ConnectedDrivingPipelineV4')

from DataSources.config import (
    DataSourceConfig,
    DateRangeConfig,
    validate_source_and_type,
    generate_cache_key,
    parse_cache_key,
    VALID_SOURCES,
    VALID_MESSAGE_TYPES,
)
from DataSources.CacheManager import CacheManager, compute_sha256, CacheEntry
from DataSources.SchemaValidator import (
    BSMSchemaValidator,
    TIMSchemaValidator,
    ValidationReport,
    get_validator,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def temp_cache_dir():
    """Create a temporary directory for cache testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def cache_manager(temp_cache_dir):
    """Create a CacheManager with temporary directory."""
    return CacheManager(
        cache_dir=temp_cache_dir,
        max_size_gb=1.0,
        ttl_days=30,
        verify_checksums=True
    )


@pytest.fixture
def bsm_validator():
    """Create a BSM validator instance."""
    return BSMSchemaValidator()


@pytest.fixture
def tim_validator():
    """Create a TIM validator instance."""
    return TIMSchemaValidator()


def create_valid_bsm_record(
    lat: float = 43.0,
    lon: float = -108.0,
    elevation: float = 2000.0,
    speed: float = 25.0,
    heading: float = 180.0,
    timestamp: str = "2021-04-01T12:00:00.000Z"
) -> Dict[str, Any]:
    """Create a valid BSM record with all required fields."""
    return {
        "metadata": {
            "recordGeneratedAt": timestamp,
            "recordGeneratedBy": "test",
            "schemaVersion": 6
        },
        "payload": {
            "data": {
                "coreData": {
                    "position": {
                        "latitude": lat,
                        "longitude": lon
                    },
                    "elevation": elevation,
                    "speed": speed,
                    "heading": heading,
                    "id": "ABCD1234",
                    "msgCnt": 1,
                    "secMark": 30000
                }
            }
        }
    }


def create_dataframe_for_cache(n_records: int, source: str = "wydot") -> pd.DataFrame:
    """Create a test DataFrame with n records for caching."""
    records = []
    base_date = date(2021, 4, 1)
    
    for i in range(n_records):
        records.append({
            "latitude": 43.0 + (i * 0.001),
            "longitude": -108.0 + (i * 0.001),
            "elevation": 2000.0 + i,
            "speed": 25.0 + (i % 50),
            "heading": (i * 10) % 360,
            "record_generated_at": f"2021-04-01T12:{i % 60:02d}:00.000Z",
            "temp_id": f"VEH{i:06d}",
            "source_marker": source  # For source isolation tests
        })
    
    return pd.DataFrame(records)


# =============================================================================
# Test 1: Empty DataFrame Handling
# =============================================================================

class TestEmptyDataFrameHandling:
    """Test that empty DataFrames are handled correctly."""
    
    def test_save_empty_dataframe(self, cache_manager):
        """Saving an empty DataFrame should work without errors."""
        df = pd.DataFrame()
        dt = date(2021, 4, 1)
        
        # Should not raise
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        assert path.exists()
        loaded = pd.read_parquet(path)
        assert len(loaded) == 0
    
    def test_load_empty_dataframe(self, cache_manager):
        """Loading an empty cached DataFrame should return empty DataFrame."""
        df = pd.DataFrame(columns=["latitude", "longitude", "speed"])
        dt = date(2021, 4, 1)
        
        cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        assert loaded is not None
        assert len(loaded) == 0
    
    def test_validator_with_empty_list(self, bsm_validator):
        """Validator batch_validate should handle empty list."""
        report = bsm_validator.batch_validate([])
        
        assert report.valid_count == 0
        assert report.invalid_count == 0
        assert report.total_count == 0
        assert len(report.errors) == 0
    
    def test_load_date_range_no_data(self, cache_manager):
        """Loading date range with no cached data should return empty DataFrame."""
        result = cache_manager.load_date_range(
            source="wydot",
            msg_type="BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 7)
        )
        
        # Should return empty DataFrame, not None
        # CacheManager returns Dask DataFrame when dask is available
        try:
            import dask.dataframe as dd
            if isinstance(result, dd.DataFrame):
                # Convert to pandas for length check
                result = result.compute()
        except ImportError:
            pass
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# =============================================================================
# Test 2: Invalid JSON in NDJSON File
# =============================================================================

class TestInvalidJSONHandling:
    """Test handling of invalid JSON in NDJSON files."""
    
    def test_parse_mixed_valid_invalid_ndjson(self, temp_cache_dir):
        """Parser should skip invalid JSON lines and keep valid ones."""
        # Create NDJSON file with mixed valid/invalid lines
        ndjson_content = [
            json.dumps(create_valid_bsm_record(lat=43.0)),  # Valid
            "{ invalid json here",  # Invalid - missing closing brace
            json.dumps(create_valid_bsm_record(lat=43.1)),  # Valid
            "",  # Empty line
            "not json at all",  # Invalid
            json.dumps(create_valid_bsm_record(lat=43.2)),  # Valid
            '{"truncated": true',  # Invalid - truncated
        ]
        
        ndjson_path = temp_cache_dir / "mixed.ndjson"
        with open(ndjson_path, 'w') as f:
            f.write('\n'.join(ndjson_content))
        
        # Parse manually (mimicking S3DataFetcher._parse_files logic)
        records = []
        with open(ndjson_path, 'r') as f:
            content = f.read().strip()
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # Skip invalid lines
        
        # Should have 3 valid records
        assert len(records) == 3
        assert records[0]['payload']['data']['coreData']['position']['latitude'] == 43.0
        assert records[1]['payload']['data']['coreData']['position']['latitude'] == 43.1
        assert records[2]['payload']['data']['coreData']['position']['latitude'] == 43.2
    
    def test_completely_invalid_ndjson_file(self, temp_cache_dir):
        """File with all invalid JSON should result in empty records."""
        ndjson_content = [
            "not valid json",
            "{ broken",
            "also broken }",
        ]
        
        ndjson_path = temp_cache_dir / "invalid.ndjson"
        with open(ndjson_path, 'w') as f:
            f.write('\n'.join(ndjson_content))
        
        records = []
        with open(ndjson_path, 'r') as f:
            content = f.read().strip()
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        
        assert len(records) == 0
    
    def test_json_with_unicode_errors(self, temp_cache_dir):
        """Handle JSON with potential encoding issues gracefully."""
        # Write some content with valid JSON but test encoding robustness
        valid_record = create_valid_bsm_record()
        valid_record["metadata"]["note"] = "Test with émojis 🚗"
        
        ndjson_path = temp_cache_dir / "unicode.ndjson"
        with open(ndjson_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(valid_record, ensure_ascii=False))
        
        with open(ndjson_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            record = json.loads(content)
        
        assert record["metadata"]["note"] == "Test with émojis 🚗"


# =============================================================================
# Test 3: Missing Required Fields in Records
# =============================================================================

class TestMissingRequiredFields:
    """Test validation of records with missing required fields."""
    
    def test_missing_latitude(self, bsm_validator):
        """Record missing latitude should be invalid."""
        record = create_valid_bsm_record()
        del record["payload"]["data"]["coreData"]["position"]["latitude"]
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False
        assert any("latitude" in err.lower() for err in errors)
    
    def test_missing_longitude(self, bsm_validator):
        """Record missing longitude should be invalid."""
        record = create_valid_bsm_record()
        del record["payload"]["data"]["coreData"]["position"]["longitude"]
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False
        assert any("longitude" in err.lower() for err in errors)
    
    def test_missing_timestamp(self, bsm_validator):
        """Record missing recordGeneratedAt should be invalid."""
        record = create_valid_bsm_record()
        del record["metadata"]["recordGeneratedAt"]
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False
        assert any("recordGeneratedAt" in err for err in errors)
    
    def test_missing_all_core_data(self, bsm_validator):
        """Record missing entire coreData should be invalid."""
        record = create_valid_bsm_record()
        del record["payload"]["data"]["coreData"]
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False
        assert len(errors) > 0
    
    def test_null_values_for_required_fields(self, bsm_validator):
        """Null values for required fields should be invalid."""
        record = create_valid_bsm_record()
        record["payload"]["data"]["coreData"]["position"]["latitude"] = None
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False
    
    def test_batch_validation_with_mixed_records(self, bsm_validator):
        """Batch validation should correctly count valid/invalid."""
        records = [
            create_valid_bsm_record(lat=43.0),  # Valid
            create_valid_bsm_record(lat=43.1),  # Valid
        ]
        
        # Add invalid record (missing latitude)
        invalid_record = create_valid_bsm_record()
        del invalid_record["payload"]["data"]["coreData"]["position"]["latitude"]
        records.append(invalid_record)
        
        # Add another invalid (missing timestamp)
        invalid_record2 = create_valid_bsm_record()
        del invalid_record2["metadata"]["recordGeneratedAt"]
        records.append(invalid_record2)
        
        records.append(create_valid_bsm_record(lat=43.2))  # Valid
        
        report = bsm_validator.batch_validate(records)
        
        assert report.valid_count == 3
        assert report.invalid_count == 2
        assert report.total_count == 5
    
    def test_missing_nested_structure(self, bsm_validator):
        """Missing intermediate nested structures should be invalid."""
        record = {
            "metadata": {"recordGeneratedAt": "2021-04-01T12:00:00.000Z"},
            "payload": {}  # Missing data entirely
        }
        
        is_valid, errors = bsm_validator.validate(record)
        
        assert is_valid is False


# =============================================================================
# Test 4: Corrupted Parquet File Detection
# =============================================================================

class TestCorruptedParquetHandling:
    """Test detection and handling of corrupted parquet files."""
    
    def test_detect_corrupted_checksum(self, cache_manager, temp_cache_dir):
        """Corrupted file with wrong checksum should be detected."""
        # Save valid data
        df = create_dataframe_for_cache(100)
        dt = date(2021, 4, 1)
        
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        # Corrupt the file by modifying it
        with open(path, 'ab') as f:
            f.write(b'\x00\x00\x00CORRUPTED')
        
        # Loading should detect corruption and return None
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        
        # With verify_checksums=True, this should detect corruption
        # The file exists but checksum won't match
        assert loaded is None
    
    def test_detect_truncated_file(self, cache_manager, temp_cache_dir):
        """Truncated parquet file should be detected."""
        # Save valid data
        df = create_dataframe_for_cache(100)
        dt = date(2021, 4, 2)
        
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        # Truncate the file
        original_size = path.stat().st_size
        with open(path, 'r+b') as f:
            f.truncate(original_size // 2)
        
        # Loading should fail
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        
        # Should detect as corrupted
        assert loaded is None
    
    def test_completely_invalid_parquet(self, cache_manager, temp_cache_dir):
        """File that's not a parquet at all should be detected."""
        dt = date(2021, 4, 3)
        key = generate_cache_key("wydot", "BSM", dt)
        
        # Create the directory structure
        cache_path = cache_manager._get_cache_path("wydot", "BSM", dt)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write invalid data
        with open(cache_path, 'w') as f:
            f.write("This is not a parquet file at all!")
        
        # Add to manifest manually
        with cache_manager._manifest_lock:
            manifest = cache_manager._load_manifest()
            manifest['entries'][key] = {
                'status': 'complete',
                'file_path': str(cache_path.relative_to(cache_manager.cache_dir)),
                'size_bytes': cache_path.stat().st_size,
                'updated_at': '2021-04-01T00:00:00',
            }
            cache_manager._save_manifest(manifest)
        
        # Loading should fail gracefully
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        assert loaded is None
    
    def test_verify_integrity_finds_corrupted(self, cache_manager):
        """verify_integrity should identify corrupted files."""
        # Save some valid data
        df = create_dataframe_for_cache(50)
        
        for day in range(1, 4):
            cache_manager.save_processed(
                df=df,
                source="wydot",
                msg_type="BSM",
                date_val=date(2021, 4, day)
            )
        
        # Corrupt one file
        path = cache_manager._get_cache_path("wydot", "BSM", date(2021, 4, 2))
        with open(path, 'ab') as f:
            f.write(b'CORRUPT')
        
        # Verify should detect the corrupted file
        results = cache_manager.verify_integrity("wydot", "BSM")
        
        corrupted_key = generate_cache_key("wydot", "BSM", date(2021, 4, 2))
        assert results[corrupted_key] is False
    
    def test_missing_file_detected(self, cache_manager):
        """Missing file (in manifest but not on disk) should be detected."""
        dt = date(2021, 4, 5)
        key = generate_cache_key("wydot", "BSM", dt)
        
        # Add to manifest without creating file
        with cache_manager._manifest_lock:
            manifest = cache_manager._load_manifest()
            manifest['entries'][key] = {
                'status': 'complete',
                'file_path': 'wydot/BSM/2021/04/05.parquet',
                'size_bytes': 1000,
                'checksum_sha256': 'fakechecksum',
                'updated_at': '2021-04-01T00:00:00',
            }
            cache_manager._save_manifest(manifest)
        
        # Should return None for missing file
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        assert loaded is None


# =============================================================================
# Test 5: Date Range with No Data (all no_data)
# =============================================================================

class TestNoDataDateRanges:
    """Test handling of date ranges where all dates have no data."""
    
    def test_mark_no_data_single_date(self, cache_manager):
        """Single date marked no_data should be tracked."""
        dt = date(2021, 4, 1)
        
        cache_manager.mark_no_data("wydot", "BSM", dt)
        
        entry = cache_manager.get_entry("wydot", "BSM", dt)
        assert entry is not None
        assert entry.status == "no_data"
    
    def test_all_dates_no_data(self, cache_manager):
        """Date range where all dates have no data."""
        start = date(2021, 4, 1)
        end = date(2021, 4, 7)
        
        # Mark all dates as no_data
        current = start
        while current <= end:
            cache_manager.mark_no_data("wydot", "BSM", current)
            current += timedelta(days=1)
        
        # Get cached dates should include no_data dates
        cached = cache_manager.get_cached_dates("wydot", "BSM", include_no_data=True)
        
        expected_dates = set()
        current = start
        while current <= end:
            expected_dates.add(current)
            current += timedelta(days=1)
        
        assert cached == expected_dates
    
    def test_missing_dates_excludes_no_data(self, cache_manager):
        """get_missing_dates should not return no_data dates."""
        # Mark some dates as no_data
        cache_manager.mark_no_data("wydot", "BSM", date(2021, 4, 1))
        cache_manager.mark_no_data("wydot", "BSM", date(2021, 4, 3))
        
        # Save actual data for one date
        df = create_dataframe_for_cache(10)
        cache_manager.save_processed(df, "wydot", "BSM", date(2021, 4, 2))
        
        # Get missing dates
        missing = cache_manager.get_missing_dates(
            "wydot", "BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 5)
        )
        
        # Only dates 4 and 5 should be missing
        assert date(2021, 4, 1) not in missing  # no_data
        assert date(2021, 4, 2) not in missing  # has data
        assert date(2021, 4, 3) not in missing  # no_data
        assert date(2021, 4, 4) in missing      # truly missing
        assert date(2021, 4, 5) in missing      # truly missing
    
    def test_load_no_data_returns_empty_df(self, cache_manager):
        """Loading a no_data date should return empty DataFrame."""
        dt = date(2021, 4, 1)
        
        cache_manager.mark_no_data("wydot", "BSM", dt)
        
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        
        assert loaded is not None
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 0
    
    def test_mixed_data_and_no_data_range(self, cache_manager):
        """Date range with mixed data and no_data entries."""
        # Save data for day 1
        df1 = create_dataframe_for_cache(100)
        cache_manager.save_processed(df1, "wydot", "BSM", date(2021, 4, 1))
        
        # Mark days 2-3 as no_data
        cache_manager.mark_no_data("wydot", "BSM", date(2021, 4, 2))
        cache_manager.mark_no_data("wydot", "BSM", date(2021, 4, 3))
        
        # Save data for day 4
        df4 = create_dataframe_for_cache(50)
        cache_manager.save_processed(df4, "wydot", "BSM", date(2021, 4, 4))
        
        # Load entire range
        result = cache_manager.load_date_range(
            "wydot", "BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 4)
        )
        
        # Should have 150 records (100 + 50, no_data dates contribute 0)
        assert len(result) == 150


# =============================================================================
# Test 6: Very Large Row Counts (10000+ records)
# =============================================================================

class TestLargeRowCounts:
    """Test handling of large datasets with 10000+ records."""
    
    def test_save_and_load_10k_records(self, cache_manager):
        """Save and load 10,000 records."""
        df = create_dataframe_for_cache(10000)
        dt = date(2021, 4, 1)
        
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        assert path.exists()
        
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        assert len(loaded) == 10000
    
    def test_save_and_load_50k_records(self, cache_manager):
        """Save and load 50,000 records."""
        df = create_dataframe_for_cache(50000)
        dt = date(2021, 4, 2)
        
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        assert path.exists()
        
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        assert len(loaded) == 50000
    
    def test_validate_10k_records_batch(self, bsm_validator):
        """Batch validate 10,000 records."""
        records = [create_valid_bsm_record(lat=43.0 + i*0.0001) for i in range(10000)]
        
        # Add some invalid records
        for i in range(0, 10000, 1000):
            del records[i]["payload"]["data"]["coreData"]["position"]["latitude"]
        
        report = bsm_validator.batch_validate(records)
        
        # 10 invalid records (at indices 0, 1000, 2000, ..., 9000)
        assert report.invalid_count == 10
        assert report.valid_count == 9990
        assert report.total_count == 10000
    
    def test_load_date_range_large(self, cache_manager):
        """Load date range with large data across multiple days."""
        # Save 5000 records per day for 5 days
        for day in range(1, 6):
            df = create_dataframe_for_cache(5000)
            cache_manager.save_processed(
                df=df,
                source="wydot",
                msg_type="BSM",
                date_val=date(2021, 4, day)
            )
        
        # Load all 5 days
        result = cache_manager.load_date_range(
            "wydot", "BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 5)
        )
        
        assert len(result) == 25000  # 5 days * 5000 records
    
    def test_manifest_tracks_record_count(self, cache_manager):
        """Manifest should track record count accurately."""
        df = create_dataframe_for_cache(12345)
        dt = date(2021, 4, 1)
        
        cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        entry = cache_manager.get_entry("wydot", "BSM", dt)
        assert entry is not None
        assert entry.row_count == 12345
    
    def test_large_data_checksum_verification(self, cache_manager):
        """Large files should have correct checksum verification."""
        df = create_dataframe_for_cache(20000)
        dt = date(2021, 4, 1)
        
        path = cache_manager.save_processed(
            df=df,
            source="wydot",
            msg_type="BSM",
            date_val=dt
        )
        
        # Get stored checksum
        entry = cache_manager.get_entry("wydot", "BSM", dt)
        stored_checksum = entry.checksum_sha256
        
        # Compute actual checksum
        actual_checksum = compute_sha256(path)
        
        assert stored_checksum == actual_checksum


# =============================================================================
# Test 7: Source Isolation
# =============================================================================

class TestSourceIsolation:
    """Test that data from different sources cannot mix."""
    
    def test_cache_keys_include_source(self):
        """Cache keys must include source as first component."""
        key_wydot = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
        key_nycdot = generate_cache_key("nycdot", "EVENT", date(2021, 4, 1))
        
        assert key_wydot.startswith("wydot/")
        assert key_nycdot.startswith("nycdot/")
        assert key_wydot != key_nycdot
    
    def test_same_date_different_sources_isolated(self, cache_manager):
        """Same date for different sources should be stored separately."""
        dt = date(2021, 4, 1)
        
        # Save data for wydot
        df_wydot = create_dataframe_for_cache(100, source="wydot")
        cache_manager.save_processed(df_wydot, "wydot", "BSM", dt)
        
        # Save data for thea
        df_thea = create_dataframe_for_cache(200, source="thea")
        cache_manager.save_processed(df_thea, "thea", "BSM", dt)
        
        # Load separately
        loaded_wydot = cache_manager.load_parquet("wydot", "BSM", dt)
        loaded_thea = cache_manager.load_parquet("thea", "BSM", dt)
        
        # Should have different counts
        assert len(loaded_wydot) == 100
        assert len(loaded_thea) == 200
        
        # Source markers should be correct
        assert all(loaded_wydot["source_marker"] == "wydot")
        assert all(loaded_thea["source_marker"] == "thea")
    
    def test_get_cached_dates_source_specific(self, cache_manager):
        """get_cached_dates should only return dates for specified source."""
        # Save data for wydot on days 1-3
        for day in range(1, 4):
            df = create_dataframe_for_cache(10)
            cache_manager.save_processed(df, "wydot", "BSM", date(2021, 4, day))
        
        # Save data for thea on days 2-5
        for day in range(2, 6):
            df = create_dataframe_for_cache(10)
            cache_manager.save_processed(df, "thea", "BSM", date(2021, 4, day))
        
        # Get cached dates for each source
        wydot_dates = cache_manager.get_cached_dates("wydot", "BSM")
        thea_dates = cache_manager.get_cached_dates("thea", "BSM")
        
        assert wydot_dates == {date(2021, 4, 1), date(2021, 4, 2), date(2021, 4, 3)}
        assert thea_dates == {date(2021, 4, 2), date(2021, 4, 3), date(2021, 4, 4), date(2021, 4, 5)}
    
    def test_validate_source_and_type_rejects_invalid_combo(self):
        """Invalid source/type combinations should be rejected."""
        # nycdot only supports EVENT
        with pytest.raises(ValueError, match="Invalid message_type"):
            validate_source_and_type("nycdot", "BSM")
        
        # wydot doesn't support EVENT
        with pytest.raises(ValueError, match="Invalid message_type"):
            validate_source_and_type("wydot", "EVENT")
    
    def test_config_enforces_source_type_combo(self):
        """DataSourceConfig should reject invalid source/type combinations."""
        # Valid combination
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01"}
        )
        assert config.source == "wydot"
        assert config.message_type == "BSM"
        
        # Invalid combination
        with pytest.raises(ValueError, match="Invalid message_type"):
            DataSourceConfig(
                source="nycdot",
                message_type="BSM",
                date_range={"start_date": "2021-04-01"}
            )
    
    def test_clear_cache_source_specific(self, cache_manager):
        """Clearing cache for one source should not affect others."""
        dt = date(2021, 4, 1)
        
        # Save data for both sources
        df_wydot = create_dataframe_for_cache(100)
        df_thea = create_dataframe_for_cache(200)
        
        cache_manager.save_processed(df_wydot, "wydot", "BSM", dt)
        cache_manager.save_processed(df_thea, "thea", "BSM", dt)
        
        # Clear only wydot
        cleared = cache_manager.clear_cache(source="wydot")
        
        # wydot should be cleared
        loaded_wydot = cache_manager.load_parquet("wydot", "BSM", dt)
        assert loaded_wydot is None
        
        # thea should still exist
        loaded_thea = cache_manager.load_parquet("thea", "BSM", dt)
        assert len(loaded_thea) == 200
    
    def test_no_cross_source_data_leakage(self, cache_manager):
        """Verify no data can leak between sources."""
        dt = date(2021, 4, 1)
        
        # Save distinctly identifiable data
        df_wydot = pd.DataFrame({
            "source": ["wydot"] * 100,
            "unique_id": [f"WYDOT_{i}" for i in range(100)]
        })
        df_thea = pd.DataFrame({
            "source": ["thea"] * 100,
            "unique_id": [f"THEA_{i}" for i in range(100)]
        })
        
        cache_manager.save_processed(df_wydot, "wydot", "BSM", dt)
        cache_manager.save_processed(df_thea, "thea", "BSM", dt)
        
        # Load wydot and verify no thea data
        loaded_wydot = cache_manager.load_parquet("wydot", "BSM", dt)
        assert all(loaded_wydot["source"] == "wydot")
        assert not any(loaded_wydot["unique_id"].str.startswith("THEA_"))
        
        # Load thea and verify no wydot data
        loaded_thea = cache_manager.load_parquet("thea", "BSM", dt)
        assert all(loaded_thea["source"] == "thea")
        assert not any(loaded_thea["unique_id"].str.startswith("WYDOT_"))
    
    def test_cache_stats_per_source(self, cache_manager):
        """Cache stats should correctly count per source."""
        # Save data for multiple sources
        for day in range(1, 4):
            df = create_dataframe_for_cache(100)
            cache_manager.save_processed(df, "wydot", "BSM", date(2021, 4, day))
        
        for day in range(1, 6):
            df = create_dataframe_for_cache(100)
            cache_manager.save_processed(df, "thea", "BSM", date(2021, 4, day))
        
        stats = cache_manager.get_stats()
        
        assert stats['source_counts']['wydot'] == 3
        assert stats['source_counts']['thea'] == 5
        assert stats['total_entries'] == 8


# =============================================================================
# Additional Edge Cases
# =============================================================================

class TestAdditionalEdgeCases:
    """Additional edge case tests."""
    
    def test_concurrent_manifest_access(self, temp_cache_dir):
        """Multiple cache managers should handle concurrent access."""
        # Create two cache managers pointing to same directory
        cm1 = CacheManager(cache_dir=temp_cache_dir, max_size_gb=1.0)
        cm2 = CacheManager(cache_dir=temp_cache_dir, max_size_gb=1.0)
        
        # Both save data
        df1 = create_dataframe_for_cache(50)
        df2 = create_dataframe_for_cache(75)
        
        cm1.save_processed(df1, "wydot", "BSM", date(2021, 4, 1))
        cm2.save_processed(df2, "wydot", "BSM", date(2021, 4, 2))
        
        # Both should see all data
        dates1 = cm1.get_cached_dates("wydot", "BSM")
        dates2 = cm2.get_cached_dates("wydot", "BSM")
        
        expected = {date(2021, 4, 1), date(2021, 4, 2)}
        assert dates1 == expected
        assert dates2 == expected
    
    def test_unicode_in_data(self, cache_manager):
        """DataFrame with unicode content should be handled."""
        df = pd.DataFrame({
            "name": ["东京", "Москва", "🚗🚙", "café"],
            "value": [1, 2, 3, 4]
        })
        dt = date(2021, 4, 1)
        
        cache_manager.save_processed(df, "wydot", "BSM", dt)
        loaded = cache_manager.load_parquet("wydot", "BSM", dt)
        
        assert loaded["name"].tolist() == ["东京", "Москва", "🚗🚙", "café"]
    
    def test_special_characters_in_values(self, bsm_validator):
        """Validator should handle special characters."""
        record = create_valid_bsm_record()
        record["metadata"]["recordGeneratedBy"] = "test/with\\special'chars\"here"
        
        is_valid, errors = bsm_validator.validate(record)
        assert is_valid is True
    
    def test_extreme_coordinate_values(self, bsm_validator):
        """Extreme but valid coordinate values."""
        # North pole
        is_valid, _ = bsm_validator.validate(
            create_valid_bsm_record(lat=90.0, lon=0.0)
        )
        assert is_valid is True
        
        # South pole
        is_valid, _ = bsm_validator.validate(
            create_valid_bsm_record(lat=-90.0, lon=0.0)
        )
        assert is_valid is True
        
        # Date line
        is_valid, _ = bsm_validator.validate(
            create_valid_bsm_record(lat=0.0, lon=180.0)
        )
        assert is_valid is True
        
        is_valid, _ = bsm_validator.validate(
            create_valid_bsm_record(lat=0.0, lon=-180.0)
        )
        assert is_valid is True
    
    def test_invalid_coordinate_values(self, bsm_validator):
        """Invalid coordinate values should fail validation."""
        # Latitude out of range
        is_valid, errors = bsm_validator.validate(
            create_valid_bsm_record(lat=91.0)
        )
        assert is_valid is False
        
        is_valid, errors = bsm_validator.validate(
            create_valid_bsm_record(lat=-91.0)
        )
        assert is_valid is False
        
        # Longitude out of range
        is_valid, errors = bsm_validator.validate(
            create_valid_bsm_record(lon=181.0)
        )
        assert is_valid is False


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
