"""
Comprehensive end-to-end tests for the DataSources pipeline.

Tests the full workflow with synthetic data:
1. Data generation
2. Mock S3 fetching
3. Cache management
4. Schema validation
5. Error handling
"""

import json
import shutil
import tempfile
import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from DataSources.config import (
    DataSourceConfig, DateRangeConfig, CacheConfig,
    validate_source_and_type, generate_cache_key, parse_cache_key,
    VALID_SOURCES, VALID_MESSAGE_TYPES
)
from DataSources.SyntheticDataGenerator import (
    SyntheticBSMGenerator, create_test_dataset, WYOMING_COORDS
)
from DataSources.MockS3DataFetcher import MockS3DataFetcher, create_mock_fetcher
from DataSources.CacheManager import CacheManager, CacheEntry
from DataSources.SchemaValidator import (
    BSMSchemaValidator, TIMSchemaValidator, ValidationReport,
    get_validator
)


class TestSyntheticDataGenerator:
    """Tests for synthetic BSM data generation."""
    
    def test_generator_initialization(self):
        """Test generator creates valid vehicle IDs."""
        gen = SyntheticBSMGenerator(seed=42, num_vehicles=10)
        assert len(gen.vehicle_ids) == 10
        # Vehicle IDs should be 8-char hex strings
        for vid in gen.vehicle_ids:
            assert len(vid) == 8
            assert all(c in '0123456789ABCDEF' for c in vid)
    
    def test_single_bsm_generation(self):
        """Test generating a single BSM record."""
        gen = SyntheticBSMGenerator(seed=42)
        timestamp = datetime(2021, 4, 1, 12, 0, 0)
        
        bsm = gen.generate_bsm(timestamp)
        
        # Check structure
        assert 'metadata' in bsm
        assert 'payload' in bsm
        
        # Check metadata
        meta = bsm['metadata']
        assert meta['schemaVersion'] == 6
        assert 'recordGeneratedAt' in meta
        assert 'serialId' in meta
        
        # Check payload
        payload = bsm['payload']
        assert payload['dataType'] == 'us.dot.its.jpo.ode.plugin.j2735.J2735Bsm'
        assert 'coreData' in payload['data']
        
        core = payload['data']['coreData']
        assert 'position' in core
        assert 'speed' in core
        assert 'heading' in core
        
        # Check position is in Wyoming
        pos = core['position']
        assert 40.5 < pos['latitude'] < 42.5  # Wyoming latitude range
        assert -108.0 < pos['longitude'] < -104.0  # Wyoming longitude range
    
    def test_batch_generation(self):
        """Test generating a batch of records."""
        gen = SyntheticBSMGenerator(seed=42)
        timestamp = datetime(2021, 4, 1, 12, 0, 0)
        
        batch = gen.generate_batch(timestamp, num_records=100, interval_ms=100)
        
        assert len(batch) == 100
        
        # Check timestamps are sequential
        times = []
        for record in batch:
            time_str = record['metadata']['recordGeneratedAt']
            times.append(time_str)
        
        # Verify all records have unique timestamps (mostly)
        assert len(set(times)) > 90  # Allow some duplicates
    
    def test_trajectory_continuity(self):
        """Test that vehicle positions are continuous over time."""
        gen = SyntheticBSMGenerator(seed=42, num_vehicles=1)
        vid = gen.vehicle_ids[0]
        
        positions = []
        for i in range(10):
            timestamp = datetime(2021, 4, 1, 12, 0, i)
            bsm = gen.generate_bsm(timestamp, vehicle_id=vid)
            pos = bsm['payload']['data']['coreData']['position']
            positions.append((pos['latitude'], pos['longitude']))
        
        # Check positions don't jump too far
        for i in range(1, len(positions)):
            lat_diff = abs(positions[i][0] - positions[i-1][0])
            lon_diff = abs(positions[i][1] - positions[i-1][1])
            
            # Reasonable movement in 1 second
            assert lat_diff < 0.01, f"Latitude jumped too far: {lat_diff}"
            assert lon_diff < 0.01, f"Longitude jumped too far: {lon_diff}"
    
    def test_ndjson_file_generation(self):
        """Test generating NDJSON file."""
        gen = SyntheticBSMGenerator(seed=42)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.json"
            timestamp = datetime(2021, 4, 1, 12, 0, 0)
            
            count = gen.generate_ndjson_file(output_path, timestamp, num_records=50)
            
            assert count == 50
            assert output_path.exists()
            
            # Verify file content
            with open(output_path) as f:
                lines = f.readlines()
            
            assert len(lines) == 50
            
            # Parse first record
            first = json.loads(lines[0])
            assert 'metadata' in first
            assert 'payload' in first
    
    def test_day_structure_generation(self):
        """Test generating full day directory structure."""
        gen = SyntheticBSMGenerator(seed=42)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            target_date = date(2021, 4, 1)
            
            stats = gen.generate_day_structure(
                base_dir,
                target_date,
                records_per_hour=10
            )
            
            # Check stats
            assert len(stats) == 24  # All hours
            assert all(v == 10 for v in stats.values())
            
            # Check directory structure
            day_dir = base_dir / "wydot" / "BSM" / "2021" / "04" / "01"
            assert day_dir.exists()
            
            # Check hour directories
            hour_dirs = list(day_dir.iterdir())
            assert len(hour_dirs) == 24
            
            # Check files in first hour
            hour_00 = day_dir / "00"
            json_files = list(hour_00.glob("*.json"))
            assert len(json_files) == 1


class TestMockS3DataFetcher:
    """Tests for MockS3DataFetcher."""
    
    @pytest.fixture
    def mock_data_dir(self):
        """Create temporary directory with mock data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            
            # Generate synthetic data
            gen = SyntheticBSMGenerator(seed=42)
            gen.generate_day_structure(
                data_dir,
                date(2021, 4, 1),
                records_per_hour=10
            )
            gen.generate_day_structure(
                data_dir,
                date(2021, 4, 2),
                records_per_hour=10
            )
            
            yield data_dir
    
    def test_fetcher_initialization(self, mock_data_dir):
        """Test MockS3DataFetcher initialization."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(start_date=date(2021, 4, 1))
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        
        assert fetcher.source == "wydot"
        assert fetcher.message_type == "BSM"
        assert fetcher.data_dir == mock_data_dir
    
    def test_fetcher_invalid_dir(self):
        """Test fetcher raises error for missing directory."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(start_date=date(2021, 4, 1))
        )
        
        with pytest.raises(FileNotFoundError):
            MockS3DataFetcher(config, Path("/nonexistent/path"))
    
    def test_list_objects_for_date(self, mock_data_dir):
        """Test listing objects for a specific date."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(start_date=date(2021, 4, 1))
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        objects = fetcher.list_objects_for_date(date(2021, 4, 1))
        
        # Should have 24 files (one per hour)
        assert len(objects) == 24
        
        # Check object structure
        for obj in objects:
            assert obj.key.startswith("wydot/BSM/2021/04/01/")
            assert obj.size > 0
            assert obj.etag.startswith('"')
            assert obj.local_path.exists()
    
    def test_list_objects_missing_date(self, mock_data_dir):
        """Test listing objects for date with no data."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(start_date=date(2021, 4, 1))
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        objects = fetcher.list_objects_for_date(date(2021, 4, 10))  # No data
        
        assert len(objects) == 0
    
    def test_read_object_json(self, mock_data_dir):
        """Test reading and parsing JSON object."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(start_date=date(2021, 4, 1))
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        objects = fetcher.list_objects_for_date(date(2021, 4, 1))
        
        # Read first object
        records = fetcher.read_object_json(objects[0])
        
        assert len(records) == 10  # records_per_hour
        
        # Verify record structure
        for record in records:
            assert 'metadata' in record
            assert 'payload' in record
    
    def test_fetch_all_records(self, mock_data_dir):
        """Test fetching all records for date range."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(
                start_date=date(2021, 4, 1),
                end_date=date(2021, 4, 2)
            )
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        records = list(fetcher.fetch_all_records())
        
        # 2 days * 24 hours * 10 records = 480
        assert len(records) == 480
    
    def test_get_date_stats(self, mock_data_dir):
        """Test getting date statistics."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(
                start_date=date(2021, 4, 1),
                end_date=date(2021, 4, 2)
            )
        )
        
        fetcher = MockS3DataFetcher(config, mock_data_dir)
        stats = fetcher.get_date_stats()
        
        assert stats['total_files'] == 48  # 24 * 2 days
        assert '2021-04-01' in stats['by_date']
        assert '2021-04-02' in stats['by_date']


class TestCacheManagerIntegration:
    """Integration tests for CacheManager with mock data."""
    
    @pytest.fixture
    def setup_dirs(self):
        """Set up temporary directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            data_dir = base / "mock_data"
            cache_dir = base / "cache"
            
            # Generate mock data
            gen = SyntheticBSMGenerator(seed=42)
            gen.generate_day_structure(data_dir, date(2021, 4, 1), records_per_hour=50)
            
            yield {
                'data_dir': data_dir,
                'cache_dir': cache_dir,
                'base': base
            }
    
    def test_cache_workflow(self, setup_dirs):
        """Test full cache workflow: add, get, verify."""
        cache = CacheManager(
            cache_dir=setup_dirs['cache_dir'],
            max_size_bytes=100 * 1024 * 1024  # 100MB
        )
        
        # Create test data
        test_data = b'{"test": "data"}\n' * 100
        cache_key = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
        
        # Add to cache
        entry = cache.put(cache_key, test_data)
        
        assert entry is not None
        assert entry.key == cache_key
        assert entry.size == len(test_data)
        
        # Verify in cache
        assert cache.contains(cache_key)
        
        # Get from cache
        retrieved = cache.get(cache_key)
        assert retrieved is not None
        assert retrieved == test_data
    
    def test_cache_isolation_by_source(self, setup_dirs):
        """Test that different sources have isolated caches."""
        cache = CacheManager(cache_dir=setup_dirs['cache_dir'])
        
        # Add data for wydot
        wydot_key = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
        cache.put(wydot_key, b'wydot data')
        
        # Add data for thea
        thea_key = generate_cache_key("thea", "BSM", date(2021, 4, 1))
        cache.put(thea_key, b'thea data')
        
        # Verify isolation
        assert cache.get(wydot_key) == b'wydot data'
        assert cache.get(thea_key) == b'thea data'
        
        # Keys should be different
        assert wydot_key != thea_key


class TestSchemaValidation:
    """Tests for schema validation with synthetic data."""
    
    def test_bsm_validator_valid_record(self):
        """Test BSM validator accepts valid record."""
        gen = SyntheticBSMGenerator(seed=42)
        bsm = gen.generate_bsm(datetime(2021, 4, 1, 12, 0, 0))
        
        validator = BSMSchemaValidator()
        is_valid, errors = validator.validate(bsm)
        
        assert is_valid, f"Validation failed: {errors}"
    
    def test_bsm_validator_batch(self):
        """Test BSM validator with batch of records."""
        gen = SyntheticBSMGenerator(seed=42)
        records = gen.generate_batch(datetime(2021, 4, 1, 12, 0, 0), num_records=100)
        
        validator = BSMSchemaValidator()
        report = validator.batch_validate(records)
        
        # All synthetic records should be valid
        assert report.valid_count == 100
        assert report.invalid_count == 0
        assert report.is_all_valid
    
    def test_bsm_validator_missing_field(self):
        """Test BSM validator catches missing required field."""
        gen = SyntheticBSMGenerator(seed=42)
        bsm = gen.generate_bsm(datetime(2021, 4, 1, 12, 0, 0))
        
        # Remove required field
        del bsm['payload']['data']['coreData']['position']
        
        validator = BSMSchemaValidator()
        is_valid, errors = validator.validate(bsm)
        
        assert not is_valid
        assert len(errors) > 0
    
    def test_bsm_validator_invalid_coordinates(self):
        """Test BSM validator catches invalid coordinates."""
        gen = SyntheticBSMGenerator(seed=42)
        bsm = gen.generate_bsm(datetime(2021, 4, 1, 12, 0, 0))
        
        # Set invalid latitude
        bsm['payload']['data']['coreData']['position']['latitude'] = 999.0
        
        validator = BSMSchemaValidator()
        is_valid, errors = validator.validate(bsm)
        
        # Should fail coordinate validation
        assert not is_valid or any('latitude' in str(e).lower() for e in errors)
    
    def test_get_validator_factory(self):
        """Test validator factory function."""
        bsm_validator = get_validator("BSM")
        tim_validator = get_validator("TIM")
        
        assert isinstance(bsm_validator, BSMSchemaValidator)
        assert isinstance(tim_validator, TIMSchemaValidator)
        
        with pytest.raises(ValueError):
            get_validator("INVALID")


class TestEndToEndPipeline:
    """Full end-to-end pipeline tests."""
    
    @pytest.fixture
    def pipeline_setup(self):
        """Set up full pipeline test environment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            data_dir = base / "mock_s3"
            cache_dir = base / "cache"
            
            # Generate multi-day dataset
            gen = SyntheticBSMGenerator(seed=42, num_vehicles=20)
            for day_offset in range(3):
                target_date = date(2021, 4, 1) + timedelta(days=day_offset)
                gen.generate_day_structure(
                    data_dir,
                    target_date,
                    records_per_hour=100
                )
            
            yield {
                'data_dir': data_dir,
                'cache_dir': cache_dir,
                'start_date': date(2021, 4, 1),
                'end_date': date(2021, 4, 3),
            }
    
    def test_full_pipeline_fetch_validate_cache(self, pipeline_setup):
        """Test complete pipeline: fetch → validate → cache."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(
                start_date=pipeline_setup['start_date'],
                end_date=pipeline_setup['end_date']
            ),
            cache=CacheConfig(directory=pipeline_setup['cache_dir'])
        )
        
        # 1. Create fetcher
        fetcher = MockS3DataFetcher(config, pipeline_setup['data_dir'])
        
        # 2. Fetch records
        all_records = list(fetcher.fetch_all_records())
        
        # 3. Validate records
        validator = BSMSchemaValidator()
        report = validator.batch_validate(all_records)
        
        # Should have 3 days * 24 hours * 100 records = 7200
        assert len(all_records) == 7200
        assert report.valid_count == 7200
        
        # 4. Cache results
        cache = CacheManager(cache_dir=pipeline_setup['cache_dir'])
        
        current_date = pipeline_setup['start_date']
        while current_date <= pipeline_setup['end_date']:
            day_records = [r for r in all_records 
                         if pipeline_setup['start_date'].isoformat() in r['metadata']['recordGeneratedAt']]
            
            cache_key = generate_cache_key("wydot", "BSM", current_date)
            
            # Would normally cache as parquet, but for test just cache JSON
            json_data = '\n'.join(json.dumps(r) for r in day_records[:100])
            cache.put(cache_key, json_data.encode())
            
            current_date += timedelta(days=1)
        
        # Verify cache
        assert cache.contains(generate_cache_key("wydot", "BSM", date(2021, 4, 1)))
    
    def test_pipeline_with_missing_days(self, pipeline_setup):
        """Test pipeline handles missing days gracefully."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(
                start_date=date(2021, 4, 1),
                end_date=date(2021, 4, 10)  # Days 4-10 don't exist
            )
        )
        
        fetcher = MockS3DataFetcher(config, pipeline_setup['data_dir'])
        
        # Should fetch only days 1-3
        records = list(fetcher.fetch_all_records())
        
        # 3 days * 24 hours * 100 records
        assert len(records) == 7200
    
    def test_pipeline_large_batch(self, pipeline_setup):
        """Test pipeline handles large batches efficiently."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range=DateRangeConfig(
                start_date=pipeline_setup['start_date'],
                end_date=pipeline_setup['end_date']
            )
        )
        
        fetcher = MockS3DataFetcher(config, pipeline_setup['data_dir'])
        validator = BSMSchemaValidator()
        
        # Process in streaming fashion
        valid_count = 0
        invalid_count = 0
        
        for record in fetcher.fetch_all_records():
            is_valid, _ = validator.validate(record)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        
        assert valid_count == 7200
        assert invalid_count == 0


class TestConfigValidation:
    """Tests for configuration validation."""
    
    def test_valid_source_type_combinations(self):
        """Test all valid source/type combinations."""
        valid_combos = [
            ("wydot", "BSM"),
            ("wydot", "TIM"),
            ("wydot_backup", "BSM"),
            ("wydot_backup", "TIM"),
            ("thea", "BSM"),
            ("thea", "TIM"),
            ("thea", "SPAT"),
            ("nycdot", "EVENT"),
        ]
        
        for source, msg_type in valid_combos:
            # Should not raise
            validate_source_and_type(source, msg_type)
    
    def test_invalid_source_type_combinations(self):
        """Test invalid combinations raise errors."""
        invalid_combos = [
            ("wydot", "SPAT"),  # SPAT is THEA only
            ("wydot", "EVENT"),  # EVENT is NYCDOT only
            ("nycdot", "BSM"),  # NYCDOT only has EVENT
            ("thea", "EVENT"),
            ("invalid", "BSM"),
        ]
        
        for source, msg_type in invalid_combos:
            with pytest.raises(ValueError):
                validate_source_and_type(source, msg_type)
    
    def test_cache_key_generation(self):
        """Test cache key generation and parsing."""
        key = generate_cache_key("wydot", "BSM", date(2021, 4, 15))
        
        assert key == "wydot/BSM/2021/04/15"
        
        # Parse back
        source, msg_type, parsed_date = parse_cache_key(key)
        
        assert source == "wydot"
        assert msg_type == "BSM"
        assert parsed_date == date(2021, 4, 15)
    
    def test_date_range_validation(self):
        """Test date range validation."""
        # Valid: absolute dates
        config = DateRangeConfig(
            start_date=date(2021, 4, 1),
            end_date=date(2021, 4, 30)
        )
        assert config.start_date < config.end_date
        
        # Valid: days_back
        config = DateRangeConfig(days_back=7)
        start, end = config.get_effective_dates()
        assert start < end
        
        # Invalid: end before start
        with pytest.raises(ValueError):
            DateRangeConfig(
                start_date=date(2021, 4, 30),
                end_date=date(2021, 4, 1)
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
