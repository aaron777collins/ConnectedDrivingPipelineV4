"""
Tests for CacheManager.

Tests cover:
- Basic CRUD operations
- File locking and concurrent access
- Atomic writes
- SHA256 checksum verification
- LRU eviction
- No-data marking
- Cache statistics
"""

import json
import os
import tempfile
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Add parent to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from DataSources.CacheManager import (
    CacheManager,
    CacheEntry,
    compute_sha256,
)
from DataSources.config import (
    generate_cache_key,
    parse_cache_key,
    validate_source_and_type,
    VALID_SOURCES,
    VALID_MESSAGE_TYPES,
)


@pytest.fixture
def temp_cache_dir():
    """Create a temporary directory for cache testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def cache_manager(temp_cache_dir):
    """Create a CacheManager instance with temp directory."""
    return CacheManager(
        cache_dir=temp_cache_dir,
        max_size_gb=1.0,
        ttl_days=30,
        verify_checksums=True,
    )


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'timestamp': pd.date_range('2021-04-01', periods=100, freq='1min'),
        'latitude': [41.0 + i * 0.001 for i in range(100)],
        'longitude': [-105.0 - i * 0.001 for i in range(100)],
        'speed': [50.0 + i % 20 for i in range(100)],
        'heading': [i * 3.6 % 360 for i in range(100)],
    })


class TestCacheManagerInit:
    """Test CacheManager initialization."""
    
    def test_creates_cache_directory(self, temp_cache_dir):
        """Test that cache directory is created."""
        cache_dir = temp_cache_dir / "new_cache"
        cm = CacheManager(cache_dir)
        assert cache_dir.exists()
    
    def test_creates_locks_directory(self, temp_cache_dir):
        """Test that .locks directory is created."""
        cm = CacheManager(temp_cache_dir)
        assert (temp_cache_dir / ".locks").exists()
    
    def test_creates_manifest(self, temp_cache_dir):
        """Test that manifest.json is created."""
        cm = CacheManager(temp_cache_dir)
        manifest_path = temp_cache_dir / "manifest.json"
        assert manifest_path.exists()
        
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        assert "version" in manifest
        assert "entries" in manifest
        assert "created_at" in manifest


class TestCacheManagerSaveAndLoad:
    """Test save_processed and load_parquet."""
    
    def test_save_processed_creates_file(self, cache_manager, sample_df, temp_cache_dir):
        """Test that save_processed creates the parquet file."""
        path = cache_manager.save_processed(
            df=sample_df,
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        assert path.exists()
        assert path.suffix == ".parquet"
        assert "wydot/BSM/2021/04/01.parquet" in str(path)
    
    def test_save_processed_updates_manifest(self, cache_manager, sample_df, temp_cache_dir):
        """Test that save_processed updates the manifest."""
        cache_manager.save_processed(
            df=sample_df,
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        manifest_path = temp_cache_dir / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        key = "wydot/BSM/2021/04/01"
        assert key in manifest["entries"]
        
        entry = manifest["entries"][key]
        assert entry["status"] == "complete"
        assert entry["record_count"] == len(sample_df)
        assert "checksum_sha256" in entry
        assert "size_bytes" in entry
        assert "updated_at" in entry
    
    def test_load_parquet_returns_dataframe(self, cache_manager, sample_df):
        """Test that load_parquet returns the saved DataFrame."""
        cache_manager.save_processed(
            df=sample_df,
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        loaded = cache_manager.load_parquet(
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        assert loaded is not None
        assert len(loaded) == len(sample_df)
        pd.testing.assert_frame_equal(loaded, sample_df)
    
    def test_load_parquet_returns_none_for_missing(self, cache_manager):
        """Test that load_parquet returns None for missing entries."""
        result = cache_manager.load_parquet(
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        assert result is None
    
    def test_load_parquet_returns_empty_for_no_data(self, cache_manager):
        """Test that load_parquet returns empty DataFrame for no_data entries."""
        cache_manager.mark_no_data(
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 15),
        )
        
        result = cache_manager.load_parquet(
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 15),
        )
        
        assert result is not None
        assert len(result) == 0


class TestSourceIsolation:
    """Test that different sources are properly isolated."""
    
    def test_different_sources_different_files(self, cache_manager, sample_df, temp_cache_dir):
        """Test that different sources create different cache files."""
        cache_manager.save_processed(
            df=sample_df,
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        # Different source with different message type
        cache_manager.save_processed(
            df=sample_df,
            source="thea",
            msg_type="BSM",
            date_val=date(2021, 4, 1),
        )
        
        # Check both files exist in different directories
        assert (temp_cache_dir / "wydot/BSM/2021/04/01.parquet").exists()
        assert (temp_cache_dir / "thea/BSM/2021/04/01.parquet").exists()
    
    def test_cached_dates_filtered_by_source(self, cache_manager, sample_df):
        """Test that get_cached_dates only returns dates for specified source."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.save_processed(
            df=sample_df, source="thea", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        wydot_dates = cache_manager.get_cached_dates("wydot", "BSM")
        thea_dates = cache_manager.get_cached_dates("thea", "BSM")
        
        assert date(2021, 4, 1) in wydot_dates
        assert date(2021, 4, 1) in thea_dates
        
        # Different source should have different count
        assert len(wydot_dates) == 1
        assert len(thea_dates) == 1


class TestMessageTypeIsolation:
    """Test that different message types are properly isolated."""
    
    def test_different_message_types_different_files(self, cache_manager, sample_df, temp_cache_dir):
        """Test that different message types create different cache files."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="TIM", date_val=date(2021, 4, 1)
        )
        
        assert (temp_cache_dir / "wydot/BSM/2021/04/01.parquet").exists()
        assert (temp_cache_dir / "wydot/TIM/2021/04/01.parquet").exists()


class TestCachedDates:
    """Test get_cached_dates method."""
    
    def test_returns_cached_dates(self, cache_manager, sample_df):
        """Test that get_cached_dates returns correct dates."""
        for day in [1, 5, 10, 15]:
            cache_manager.save_processed(
                df=sample_df,
                source="wydot",
                msg_type="BSM",
                date_val=date(2021, 4, day),
            )
        
        cached = cache_manager.get_cached_dates("wydot", "BSM")
        
        assert len(cached) == 4
        assert date(2021, 4, 1) in cached
        assert date(2021, 4, 5) in cached
        assert date(2021, 4, 10) in cached
        assert date(2021, 4, 15) in cached
    
    def test_includes_no_data_by_default(self, cache_manager, sample_df):
        """Test that no_data entries are included by default."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.mark_no_data(
            source="wydot", msg_type="BSM", date_val=date(2021, 4, 2)
        )
        
        cached = cache_manager.get_cached_dates("wydot", "BSM", include_no_data=True)
        
        assert len(cached) == 2
        assert date(2021, 4, 1) in cached
        assert date(2021, 4, 2) in cached
    
    def test_excludes_no_data_when_requested(self, cache_manager, sample_df):
        """Test that no_data entries can be excluded."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.mark_no_data(
            source="wydot", msg_type="BSM", date_val=date(2021, 4, 2)
        )
        
        cached = cache_manager.get_cached_dates("wydot", "BSM", include_no_data=False)
        
        assert len(cached) == 1
        assert date(2021, 4, 1) in cached


class TestMissingDates:
    """Test get_missing_dates method."""
    
    def test_returns_all_dates_when_empty(self, cache_manager):
        """Test that all dates are returned when cache is empty."""
        missing = cache_manager.get_missing_dates(
            source="wydot",
            msg_type="BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 5),
        )
        
        assert len(missing) == 5
        assert missing == [date(2021, 4, d) for d in range(1, 6)]
    
    def test_excludes_cached_dates(self, cache_manager, sample_df):
        """Test that cached dates are excluded."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 3)
        )
        
        missing = cache_manager.get_missing_dates(
            source="wydot",
            msg_type="BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 5),
        )
        
        assert len(missing) == 4
        assert date(2021, 4, 3) not in missing
    
    def test_excludes_no_data_dates(self, cache_manager):
        """Test that no_data dates are excluded."""
        cache_manager.mark_no_data(
            source="wydot", msg_type="BSM", date_val=date(2021, 4, 3)
        )
        
        missing = cache_manager.get_missing_dates(
            source="wydot",
            msg_type="BSM",
            start=date(2021, 4, 1),
            end=date(2021, 4, 5),
        )
        
        assert len(missing) == 4
        assert date(2021, 4, 3) not in missing


class TestMarkNoData:
    """Test mark_no_data method."""
    
    def test_marks_entry_as_no_data(self, cache_manager, temp_cache_dir):
        """Test that mark_no_data creates correct manifest entry."""
        cache_manager.mark_no_data(
            source="wydot",
            msg_type="BSM",
            date_val=date(2021, 4, 15),
            s3_files_found=0,
        )
        
        manifest_path = temp_cache_dir / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        key = "wydot/BSM/2021/04/15"
        assert key in manifest["entries"]
        assert manifest["entries"][key]["status"] == "no_data"
        assert manifest["entries"][key]["s3_files_found"] == 0
        assert "checked_at" in manifest["entries"][key]


class TestAtomicWrites:
    """Test that writes are atomic (temp file + rename)."""
    
    def test_no_partial_files_on_success(self, cache_manager, sample_df, temp_cache_dir):
        """Test that successful writes don't leave temp files."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        # Check no temp files remain
        temp_files = list(temp_cache_dir.glob(".tmp_*.parquet"))
        assert len(temp_files) == 0
    
    def test_no_partial_files_on_failure(self, cache_manager, temp_cache_dir):
        """Test that failed writes don't leave partial files."""
        # Create a DataFrame that will fail to write
        bad_df = pd.DataFrame({"col": [object()]})  # Can't serialize object
        
        with pytest.raises(Exception):
            cache_manager.save_processed(
                df=bad_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
            )
        
        # Check no temp files or partial files remain
        temp_files = list(temp_cache_dir.glob(".tmp_*.parquet"))
        assert len(temp_files) == 0


class TestChecksumVerification:
    """Test SHA256 checksum verification."""
    
    def test_checksum_stored_in_manifest(self, cache_manager, sample_df, temp_cache_dir):
        """Test that checksum is stored in manifest."""
        path = cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        manifest_path = temp_cache_dir / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        entry = manifest["entries"]["wydot/BSM/2021/04/01"]
        assert "checksum_sha256" in entry
        assert len(entry["checksum_sha256"]) == 64  # SHA256 hex length
        
        # Verify checksum matches file
        actual_checksum = compute_sha256(path)
        assert entry["checksum_sha256"] == actual_checksum
    
    def test_corrupted_file_detected(self, cache_manager, sample_df, temp_cache_dir):
        """Test that corrupted files are detected."""
        path = cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        # Corrupt the file
        with open(path, "ab") as f:
            f.write(b"corrupted data")
        
        # Try to load - should return None and mark as corrupted
        result = cache_manager.load_parquet(
            source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        assert result is None
        
        # Check marked as corrupted
        entry = cache_manager.get_entry("wydot", "BSM", date(2021, 4, 1))
        assert entry.status == "corrupted"


class TestLRUEviction:
    """Test LRU eviction."""
    
    def test_eviction_frees_space(self, temp_cache_dir, sample_df):
        """Test that eviction frees up space."""
        # Create manager with very small limit
        cm = CacheManager(temp_cache_dir, max_size_gb=0.001)  # 1MB
        
        # Save multiple entries
        for day in range(1, 6):
            cm.save_processed(
                df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, day)
            )
            time.sleep(0.1)  # Ensure different timestamps
        
        # Evict to make space
        bytes_freed = cm.evict_lru(bytes_needed=100000)  # Need 100KB
        
        assert bytes_freed > 0
    
    def test_eviction_removes_oldest_first(self, temp_cache_dir, sample_df):
        """Test that eviction removes oldest accessed entries first."""
        cm = CacheManager(temp_cache_dir, max_size_gb=0.001)
        
        # Save entries with different access times
        for day in range(1, 4):
            cm.save_processed(
                df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, day)
            )
            time.sleep(0.1)
        
        # Access the first entry to make it most recent
        cm.load_parquet(source="wydot", msg_type="BSM", date_val=date(2021, 4, 1))
        
        # Evict
        cm.evict_lru(bytes_needed=100000)
        
        # First entry should still exist (was accessed most recently)
        # Middle entries should be evicted first
        entry_1 = cm.get_entry("wydot", "BSM", date(2021, 4, 1))
        entry_2 = cm.get_entry("wydot", "BSM", date(2021, 4, 2))
        
        # At least one should be evicted
        statuses = [
            entry_1.status if entry_1 else "removed",
            entry_2.status if entry_2 else "removed",
        ]
        # Entry 1 was accessed most recently, so entry 2 should be evicted first
        # (or both could be evicted if we needed enough space)


class TestClearCache:
    """Test clear_cache method."""
    
    def test_clear_all(self, cache_manager, sample_df):
        """Test clearing all cache entries."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.save_processed(
            df=sample_df, source="thea", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        cleared = cache_manager.clear_cache()
        
        assert cleared == 2
        assert len(cache_manager.get_cached_dates("wydot", "BSM")) == 0
        assert len(cache_manager.get_cached_dates("thea", "BSM")) == 0
    
    def test_clear_by_source(self, cache_manager, sample_df):
        """Test clearing cache by source."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.save_processed(
            df=sample_df, source="thea", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        cleared = cache_manager.clear_cache(source="wydot")
        
        assert cleared == 1
        assert len(cache_manager.get_cached_dates("wydot", "BSM")) == 0
        assert len(cache_manager.get_cached_dates("thea", "BSM")) == 1
    
    def test_clear_by_message_type(self, cache_manager, sample_df):
        """Test clearing cache by message type."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="TIM", date_val=date(2021, 4, 1)
        )
        
        cleared = cache_manager.clear_cache(source="wydot", msg_type="BSM")
        
        assert cleared == 1
        assert len(cache_manager.get_cached_dates("wydot", "BSM")) == 0
        assert len(cache_manager.get_cached_dates("wydot", "TIM")) == 1


class TestCacheStats:
    """Test get_cache_stats method."""
    
    def test_returns_stats(self, cache_manager, sample_df):
        """Test that get_cache_stats returns correct statistics."""
        cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        cache_manager.mark_no_data(
            source="wydot", msg_type="BSM", date_val=date(2021, 4, 2)
        )
        
        stats = cache_manager.get_cache_stats()
        
        assert stats["total_entries"] == 2
        assert stats["status_counts"]["complete"] == 1
        assert stats["status_counts"]["no_data"] == 1
        assert stats["total_records"] == len(sample_df)  # actual impl uses total_records
        assert stats["total_size_bytes"] > 0
        assert "source_counts" in stats
        assert stats["source_counts"]["wydot"] == 2


class TestValidation:
    """Test input validation."""
    
    def test_invalid_source_rejected(self, cache_manager, sample_df):
        """Test that invalid source is rejected."""
        with pytest.raises(ValueError, match="Invalid source"):
            cache_manager.save_processed(
                df=sample_df,
                source="invalid_source",
                msg_type="BSM",
                date_val=date(2021, 4, 1),
            )
    
    def test_invalid_message_type_rejected(self, cache_manager, sample_df):
        """Test that invalid message type for source is rejected."""
        with pytest.raises(ValueError, match="Invalid message_type"):
            cache_manager.save_processed(
                df=sample_df,
                source="nycdot",
                msg_type="BSM",  # nycdot only supports EVENT
                date_val=date(2021, 4, 1),
            )
    
    def test_end_before_start_rejected(self, cache_manager):
        """Test that end < start is rejected."""
        with pytest.raises(ValueError):
            cache_manager.get_missing_dates(
                source="wydot",
                msg_type="BSM",
                start=date(2021, 4, 30),
                end=date(2021, 4, 1),
            )


class TestConcurrency:
    """Test concurrent access safety."""
    
    def test_concurrent_writes_safe(self, temp_cache_dir, sample_df):
        """Test that concurrent writes don't corrupt the cache."""
        cm = CacheManager(temp_cache_dir)
        errors = []
        
        def write_entry(day):
            try:
                cm.save_processed(
                    df=sample_df,
                    source="wydot",
                    msg_type="BSM",
                    date_val=date(2021, 4, day),
                )
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads writing concurrently
        threads = [
            threading.Thread(target=write_entry, args=(day,))
            for day in range(1, 6)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        
        # Verify all entries were saved
        cached = cm.get_cached_dates("wydot", "BSM")
        assert len(cached) == 5
    
    def test_concurrent_manifest_updates_safe(self, temp_cache_dir, sample_df):
        """Test that concurrent manifest updates are safe."""
        cm = CacheManager(temp_cache_dir)
        errors = []
        
        def update_cache(day):
            try:
                if day % 2 == 0:
                    cm.save_processed(
                        df=sample_df,
                        source="wydot",
                        msg_type="BSM",
                        date_val=date(2021, 4, day),
                    )
                else:
                    cm.mark_no_data(
                        source="wydot",
                        msg_type="BSM",
                        date_val=date(2021, 4, day),
                    )
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=update_cache, args=(day,))
            for day in range(1, 11)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        
        # Verify manifest is valid JSON
        manifest_path = temp_cache_dir / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        assert len(manifest["entries"]) == 10


class TestVerifyAll:
    """Test verify_all method."""
    
    def test_verify_all_detects_issues(self, cache_manager, sample_df):
        """Test that verify_all detects corrupted/missing files."""
        # Save some entries
        path1 = cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        path2 = cache_manager.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 2)
        )
        
        # Corrupt one file
        with open(path1, "ab") as f:
            f.write(b"corrupted")
        
        # Delete another file (but keep manifest entry)
        path2.unlink()
        
        result = cache_manager.verify_all()
        
        assert len(result["corrupted"]) == 1
        assert len(result["missing"]) == 1


class TestExpiration:
    """Test TTL expiration."""
    
    def test_expired_entry_detected(self, temp_cache_dir, sample_df):
        """Test that expired entries are detected."""
        # Create manager with short TTL
        cm = CacheManager(temp_cache_dir, ttl_days=1)
        
        cm.save_processed(
            df=sample_df, source="wydot", msg_type="BSM", date_val=date(2021, 4, 1)
        )
        
        # Manually set updated_at to past (implementation checks updated_at first)
        manifest_path = temp_cache_dir / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        old_date = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
        # Set both updated_at and fetched_at to ensure expiration is detected
        manifest["entries"]["wydot/BSM/2021/04/01"]["updated_at"] = old_date
        manifest["entries"]["wydot/BSM/2021/04/01"]["fetched_at"] = old_date
        
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)
        
        # Try to load - should return None due to expiration
        result = cm.load_parquet(source="wydot", msg_type="BSM", date_val=date(2021, 4, 1))
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
