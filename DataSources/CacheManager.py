"""
CacheManager - Manages local cache of downloaded and processed data.

Features:
- File locking for concurrent access
- Atomic manifest updates
- LRU eviction when disk full
- Tracks "no data" vs "not fetched"
- Integrity verification

Cache Structure:
    cache/
    ├── manifest.json           # Tracking metadata (with lock)
    ├── wydot/
    │   └── BSM/
    │       └── 2021/04/01.parquet
    └── .locks/                 # Lock files for concurrent access
"""

import hashlib
import json
import logging
import shutil
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set

import pandas as pd

try:
    from filelock import FileLock
except ImportError:
    # Fallback for systems without filelock
    class FileLock:
        def __init__(self, path, timeout=60):
            self.path = path
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

from .config import generate_cache_key, parse_cache_key

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Manages local cache of downloaded and processed CV Pilot data.
    
    Features:
    - File locking for concurrent access
    - Atomic manifest updates
    - LRU eviction when disk full
    - Tracks "no data" vs "not fetched" to avoid re-fetching empty dates
    
    Usage:
        cache = CacheManager(Path("data/cache"))
        
        # Check what's cached
        cached = cache.get_cached_dates("wydot", "BSM")
        missing = cache.get_missing_dates("wydot", "BSM", start, end)
        
        # Save processed data
        cache.save_processed(df, "wydot", "BSM", date(2021, 4, 1))
        
        # Mark empty date
        cache.mark_no_data("wydot", "BSM", date(2021, 4, 2))
        
        # Load data
        df = cache.load_date_range("wydot", "BSM", start, end)
    """
    
    MANIFEST_FILENAME = "manifest.json"
    LOCKS_DIR = ".locks"
    
    def __init__(self, cache_dir: Path, max_size_gb: float = 50.0):
        """
        Initialize CacheManager.
        
        Args:
            cache_dir: Root directory for cache storage
            max_size_gb: Maximum cache size in GB (triggers LRU eviction)
        """
        self.cache_dir = Path(cache_dir)
        self.max_size_gb = max_size_gb
        self.max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)
        
        # Create directories
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.lock_dir = self.cache_dir / self.LOCKS_DIR
        self.lock_dir.mkdir(exist_ok=True)
        
        # Manifest lock for atomic updates
        self._manifest_lock = FileLock(self.lock_dir / "manifest.lock", timeout=60)
        
        # Initialize manifest if needed
        self._init_manifest()
        
        logger.info(f"Initialized CacheManager at {self.cache_dir} (max: {max_size_gb}GB)")
    
    def _init_manifest(self) -> None:
        """Initialize manifest file if it doesn't exist."""
        manifest_path = self.cache_dir / self.MANIFEST_FILENAME
        
        if not manifest_path.exists():
            manifest = {
                "version": "1.0",
                "created_at": datetime.utcnow().isoformat(),
                "entries": {}
            }
            self._save_manifest(manifest)
    
    def _load_manifest(self) -> Dict:
        """Load manifest from disk."""
        manifest_path = self.cache_dir / self.MANIFEST_FILENAME
        
        try:
            with open(manifest_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"version": "1.0", "entries": {}}
    
    def _save_manifest(self, manifest: Dict) -> None:
        """
        Save manifest atomically using temp file + rename.
        
        This ensures the manifest is never corrupted by partial writes.
        """
        manifest_path = self.cache_dir / self.MANIFEST_FILENAME
        temp_path = self.cache_dir / f".tmp_manifest_{uuid.uuid4()}.json"
        
        try:
            with open(temp_path, 'w') as f:
                json.dump(manifest, f, indent=2, default=str)
            temp_path.rename(manifest_path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
    
    def _get_lock(self, key: str) -> FileLock:
        """Get a lock for a specific cache key."""
        safe_key = key.replace('/', '_')
        lock_file = self.lock_dir / f"{safe_key}.lock"
        return FileLock(lock_file, timeout=60)
    
    def _get_cache_path(self, source: str, msg_type: str, date_val: date) -> Path:
        """Get the parquet cache path for a specific date."""
        return (
            self.cache_dir / source / msg_type / 
            str(date_val.year) / f"{date_val.month:02d}" / f"{date_val.day:02d}.parquet"
        )
    
    def _compute_checksum(self, path: Path) -> str:
        """Compute SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def get_cached_dates(self, source: str, msg_type: str) -> Set[date]:
        """
        Return set of dates already in cache.
        
        Includes both dates with data and dates marked as 'no_data'.
        
        Args:
            source: Data source (e.g., 'wydot')
            msg_type: Message type (e.g., 'BSM')
        
        Returns:
            Set of dates that have been processed (have data or confirmed empty)
        """
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        cached_dates = set()
        prefix = f"{source}/{msg_type}/"
        
        for key, entry in manifest.get('entries', {}).items():
            if key.startswith(prefix):
                try:
                    _, _, dt = parse_cache_key(key)
                    cached_dates.add(dt)
                except ValueError:
                    continue
        
        return cached_dates
    
    def get_missing_dates(
        self,
        source: str,
        msg_type: str,
        start: date,
        end: date
    ) -> List[date]:
        """
        Return dates not yet cached in the given range.
        
        Args:
            source: Data source
            msg_type: Message type
            start: Start date (inclusive)
            end: End date (inclusive)
        
        Returns:
            List of dates that need to be fetched
        """
        cached = self.get_cached_dates(source, msg_type)
        missing = []
        
        current = start
        while current <= end:
            if current not in cached:
                missing.append(current)
            current += timedelta(days=1)
        
        return missing
    
    def save_processed(
        self,
        df: pd.DataFrame,
        source: str,
        msg_type: str,
        date_val: date
    ) -> Path:
        """
        Save processed DataFrame to Parquet cache.
        
        Uses atomic write: temp file → rename.
        
        Args:
            df: DataFrame to cache
            source: Data source
            msg_type: Message type
            date_val: Date of the data
        
        Returns:
            Path to saved parquet file
        """
        key = generate_cache_key(source, msg_type, date_val)
        cache_path = self._get_cache_path(source, msg_type, date_val)
        temp_path = cache_path.parent / f".tmp_{uuid.uuid4()}.parquet"
        
        # Ensure directory exists
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        with self._get_lock(key):
            try:
                # Check if we need to evict before writing
                estimated_size = len(df) * 500  # Rough estimate
                self._ensure_space(estimated_size)
                
                # Write to temp file
                df.to_parquet(temp_path, compression='snappy', index=False)
                
                # Atomic rename
                temp_path.rename(cache_path)
                
                # Update manifest
                self._update_manifest_entry(
                    key=key,
                    status='complete',
                    file_path=str(cache_path.relative_to(self.cache_dir)),
                    checksum=self._compute_checksum(cache_path),
                    size_bytes=cache_path.stat().st_size,
                    record_count=len(df)
                )
                
                logger.debug(f"Cached {len(df)} records for {key}")
                return cache_path
                
            except Exception:
                temp_path.unlink(missing_ok=True)
                raise
    
    def mark_no_data(self, source: str, msg_type: str, date_val: date) -> None:
        """
        Mark a date as having no data in S3.
        
        This prevents re-fetching empty dates.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date with no data
        """
        key = generate_cache_key(source, msg_type, date_val)
        
        self._update_manifest_entry(
            key=key,
            status='no_data',
            file_path=None,
            checksum=None,
            size_bytes=0,
            record_count=0
        )
        
        logger.debug(f"Marked {key} as no_data")
    
    def _update_manifest_entry(
        self,
        key: str,
        status: str,
        file_path: Optional[str],
        checksum: Optional[str],
        size_bytes: int,
        record_count: int
    ) -> None:
        """Update a single manifest entry atomically."""
        with self._manifest_lock:
            manifest = self._load_manifest()
            
            manifest['entries'][key] = {
                'status': status,
                'file_path': file_path,
                'checksum_sha256': checksum,
                'size_bytes': size_bytes,
                'record_count': record_count,
                'updated_at': datetime.utcnow().isoformat(),
                'accessed_at': datetime.utcnow().isoformat(),
            }
            
            self._save_manifest(manifest)
    
    def _update_access_time(self, key: str) -> None:
        """Update the access time for LRU tracking."""
        with self._manifest_lock:
            manifest = self._load_manifest()
            
            if key in manifest.get('entries', {}):
                manifest['entries'][key]['accessed_at'] = datetime.utcnow().isoformat()
                self._save_manifest(manifest)
    
    def load_date_range(
        self,
        source: str,
        msg_type: str,
        start: date,
        end: date
    ):
        """
        Load cached data for a date range.
        
        Args:
            source: Data source
            msg_type: Message type
            start: Start date (inclusive)
            end: End date (inclusive)
        
        Returns:
            Dask DataFrame for lazy loading (or pandas DataFrame if dask unavailable)
        """
        try:
            import dask.dataframe as dd
            use_dask = True
        except ImportError:
            use_dask = False
            logger.warning("Dask not available, using pandas")
        
        # Collect all parquet files in range
        parquet_files = []
        current = start
        
        while current <= end:
            key = generate_cache_key(source, msg_type, current)
            cache_path = self._get_cache_path(source, msg_type, current)
            
            if cache_path.exists():
                parquet_files.append(cache_path)
                self._update_access_time(key)
            
            current += timedelta(days=1)
        
        if not parquet_files:
            logger.warning(f"No cached data found for {source}/{msg_type} from {start} to {end}")
            # Return empty DataFrame with expected schema
            return pd.DataFrame() if not use_dask else dd.from_pandas(pd.DataFrame(), npartitions=1)
        
        logger.info(f"Loading {len(parquet_files)} cached files")
        
        if use_dask:
            return dd.read_parquet(parquet_files)
        else:
            dfs = [pd.read_parquet(f) for f in parquet_files]
            return pd.concat(dfs, ignore_index=True)
    
    def get_cache_size(self) -> int:
        """Get total size of cache in bytes."""
        total = 0
        for path in self.cache_dir.rglob('*.parquet'):
            total += path.stat().st_size
        return total
    
    def _ensure_space(self, bytes_needed: int) -> None:
        """Ensure enough space by evicting LRU entries if necessary."""
        current_size = self.get_cache_size()
        
        if current_size + bytes_needed <= self.max_size_bytes:
            return
        
        # Need to evict
        bytes_to_free = (current_size + bytes_needed) - self.max_size_bytes
        logger.info(f"Cache full, need to free {bytes_to_free / 1024 / 1024:.1f}MB")
        
        self.evict_lru(bytes_to_free)
    
    def evict_lru(self, bytes_needed: int) -> None:
        """
        Evict least-recently-used entries to free space.
        
        Args:
            bytes_needed: Minimum bytes to free
        """
        with self._manifest_lock:
            manifest = self._load_manifest()
            entries = manifest.get('entries', {})
            
            # Sort by access time (oldest first)
            sorted_entries = sorted(
                [(k, v) for k, v in entries.items() if v.get('status') == 'complete'],
                key=lambda x: x[1].get('accessed_at', ''),
            )
            
            bytes_freed = 0
            evicted = []
            
            for key, entry in sorted_entries:
                if bytes_freed >= bytes_needed:
                    break
                
                file_path = entry.get('file_path')
                if file_path:
                    full_path = self.cache_dir / file_path
                    if full_path.exists():
                        size = entry.get('size_bytes', full_path.stat().st_size)
                        full_path.unlink()
                        bytes_freed += size
                        evicted.append(key)
                        logger.debug(f"Evicted {key} ({size / 1024 / 1024:.1f}MB)")
            
            # Remove from manifest
            for key in evicted:
                del manifest['entries'][key]
            
            self._save_manifest(manifest)
            
            logger.info(f"Evicted {len(evicted)} entries, freed {bytes_freed / 1024 / 1024:.1f}MB")
    
    def clear_cache(
        self,
        source: Optional[str] = None,
        msg_type: Optional[str] = None
    ) -> None:
        """
        Clear cache entries.
        
        Args:
            source: If specified, only clear this source
            msg_type: If specified, only clear this message type
        """
        with self._manifest_lock:
            manifest = self._load_manifest()
            entries = manifest.get('entries', {})
            
            keys_to_delete = []
            
            for key, entry in entries.items():
                # Check if this entry matches filter
                try:
                    entry_source, entry_type, _ = parse_cache_key(key)
                except ValueError:
                    continue
                
                if source and entry_source != source:
                    continue
                if msg_type and entry_type != msg_type:
                    continue
                
                # Delete file
                file_path = entry.get('file_path')
                if file_path:
                    full_path = self.cache_dir / file_path
                    full_path.unlink(missing_ok=True)
                
                keys_to_delete.append(key)
            
            # Remove from manifest
            for key in keys_to_delete:
                del manifest['entries'][key]
            
            self._save_manifest(manifest)
            
            logger.info(f"Cleared {len(keys_to_delete)} cache entries")
    
    def verify_integrity(self, source: str, msg_type: str) -> Dict[str, bool]:
        """
        Verify integrity of all cached files for a source/type.
        
        Returns:
            Dict mapping cache keys to verification status
        """
        results = {}
        
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        prefix = f"{source}/{msg_type}/"
        
        for key, entry in manifest.get('entries', {}).items():
            if not key.startswith(prefix):
                continue
            
            if entry.get('status') != 'complete':
                results[key] = True  # no_data entries are always "valid"
                continue
            
            file_path = entry.get('file_path')
            if not file_path:
                results[key] = False
                continue
            
            full_path = self.cache_dir / file_path
            
            if not full_path.exists():
                results[key] = False
                continue
            
            # Verify checksum
            expected = entry.get('checksum_sha256')
            if expected:
                actual = self._compute_checksum(full_path)
                results[key] = actual == expected
            else:
                # No checksum stored, just verify file exists and is readable
                try:
                    pd.read_parquet(full_path)
                    results[key] = True
                except Exception:
                    results[key] = False
        
        return results
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        entries = manifest.get('entries', {})
        
        complete = sum(1 for e in entries.values() if e.get('status') == 'complete')
        no_data = sum(1 for e in entries.values() if e.get('status') == 'no_data')
        total_size = sum(e.get('size_bytes', 0) for e in entries.values())
        total_records = sum(e.get('record_count', 0) for e in entries.values())
        
        return {
            'total_entries': len(entries),
            'complete_entries': complete,
            'no_data_entries': no_data,
            'total_size_bytes': total_size,
            'total_size_gb': total_size / (1024 ** 3),
            'total_records': total_records,
            'max_size_gb': self.max_size_gb,
            'usage_percent': (total_size / self.max_size_bytes) * 100 if self.max_size_bytes else 0,
        }
