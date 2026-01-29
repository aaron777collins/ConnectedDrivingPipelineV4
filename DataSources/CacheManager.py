"""
CacheManager - Manages local cache of downloaded and processed data.

Features:
- File locking for concurrent access (filelock)
- Atomic manifest updates
- LRU eviction when disk full
- Tracks status: complete, downloading, no_data, corrupted, expired
- SHA256 checksum verification
- TTL-based expiration

Cache Structure:
    cache/
    ├── manifest.json           # Tracking metadata (with lock)
    ├── .locks/                 # Lock files for concurrent access
    └── {source}/{message_type}/{year}/{month}/{day}.parquet
"""

import hashlib
import json
import logging
import shutil
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

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

from .config import (
    generate_cache_key,
    parse_cache_key,
    validate_source_and_type,
    VALID_SOURCES,
    VALID_MESSAGE_TYPES,
)

logger = logging.getLogger(__name__)

# Manifest version - bump when format changes
MANIFEST_VERSION = 2

# Valid status values for cache entries
VALID_STATUSES = {'complete', 'downloading', 'no_data', 'corrupted', 'expired', 'failed'}


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


class CacheEntry:
    """Represents a single cache entry from the manifest."""
    
    def __init__(self, data: Dict[str, Any]):
        self.status: str = data.get('status', 'unknown')
        self.file_path: Optional[str] = data.get('file_path')
        self.row_count: Optional[int] = data.get('record_count') or data.get('row_count')
        self.file_size_bytes: Optional[int] = data.get('size_bytes') or data.get('file_size_bytes')
        self.checksum_sha256: Optional[str] = data.get('checksum_sha256')
        self.fetched_at: Optional[str] = data.get('updated_at') or data.get('fetched_at')
        self.checked_at: Optional[str] = data.get('checked_at')
        self.last_accessed: Optional[str] = data.get('accessed_at') or data.get('last_accessed')
        self.schema_version: Optional[str] = data.get('schema_version')
        self.config_hash: Optional[str] = data.get('config_hash')
        self.s3_files_found: Optional[int] = data.get('s3_files_found')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to dictionary for serialization."""
        result = {'status': self.status}
        if self.file_path is not None:
            result['file_path'] = self.file_path
        if self.row_count is not None:
            result['record_count'] = self.row_count
        if self.file_size_bytes is not None:
            result['size_bytes'] = self.file_size_bytes
        if self.checksum_sha256 is not None:
            result['checksum_sha256'] = self.checksum_sha256
        if self.fetched_at is not None:
            result['updated_at'] = self.fetched_at
        if self.checked_at is not None:
            result['checked_at'] = self.checked_at
        if self.last_accessed is not None:
            result['accessed_at'] = self.last_accessed
        if self.schema_version is not None:
            result['schema_version'] = self.schema_version
        if self.config_hash is not None:
            result['config_hash'] = self.config_hash
        if self.s3_files_found is not None:
            result['s3_files_found'] = self.s3_files_found
        return result


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
    
    def __init__(
        self,
        cache_dir: Path,
        max_size_gb: float = 50.0,
        ttl_days: int = 30,
        verify_checksums: bool = True,
        lock_timeout: int = 60,
        schema_version: str = "6",
    ):
        """
        Initialize CacheManager.
        
        Args:
            cache_dir: Root directory for cache storage
            max_size_gb: Maximum cache size in GB (triggers LRU eviction)
            ttl_days: Time-to-live for cache entries in days (default 30)
            verify_checksums: Whether to verify SHA256 checksums on load (default True)
            lock_timeout: Timeout for lock acquisition in seconds (default 60)
            schema_version: Current schema version for cache entries
        """
        self.cache_dir = Path(cache_dir)
        self.max_size_gb = max_size_gb
        self.max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)
        self.ttl_days = ttl_days
        self.verify_checksums = verify_checksums
        self.lock_timeout = lock_timeout
        self.schema_version = schema_version
        
        # Create directories
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.lock_dir = self.cache_dir / self.LOCKS_DIR
        self.lock_dir.mkdir(exist_ok=True)
        
        # Manifest lock for atomic updates
        self._manifest_lock = FileLock(self.lock_dir / "manifest.lock", timeout=self.lock_timeout)
        
        # Initialize manifest if needed
        self._init_manifest()
        
        logger.info(f"Initialized CacheManager at {self.cache_dir} (max: {max_size_gb}GB, ttl: {ttl_days}d)")
    
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
    
    def get_cached_dates(
        self,
        source: str,
        msg_type: str,
        include_no_data: bool = True,
    ) -> Set[date]:
        """
        Return set of dates already in cache.
        
        Args:
            source: Data source (e.g., 'wydot')
            msg_type: Message type (e.g., 'BSM')
            include_no_data: Include dates marked as having no data (default True)
        
        Returns:
            Set of dates that have been processed (have data or confirmed empty)
        """
        # Validate inputs
        validate_source_and_type(source, msg_type)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        cached_dates = set()
        prefix = f"{source}/{msg_type}/"
        
        for key, entry in manifest.get('entries', {}).items():
            if key.startswith(prefix):
                status = entry.get('status', 'unknown')
                
                # Skip non-complete unless it's no_data and we're including those
                if status == 'complete':
                    try:
                        _, _, dt = parse_cache_key(key)
                        cached_dates.add(dt)
                    except ValueError:
                        continue
                elif status == 'no_data' and include_no_data:
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
        
        Also returns dates with corrupted/expired/failed status that need re-fetch.
        
        Args:
            source: Data source
            msg_type: Message type
            start: Start date (inclusive)
            end: End date (inclusive)
        
        Returns:
            List of dates that need to be fetched
        """
        # Validate inputs
        validate_source_and_type(source, msg_type)
        
        if end < start:
            raise ValueError("end date must be >= start date")
        
        # Get all dates in range
        all_dates = []
        current = start
        while current <= end:
            all_dates.append(current)
            current += timedelta(days=1)
        
        # Get cached dates (including no_data)
        cached = self.get_cached_dates(source, msg_type, include_no_data=True)
        
        # Check for entries that need re-fetch (corrupted, expired, failed)
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        needs_refetch: Set[date] = set()
        for dt in all_dates:
            key = generate_cache_key(source, msg_type, dt)
            entry_data = manifest.get('entries', {}).get(key)
            
            if entry_data is None:
                continue
            
            status = entry_data.get('status', 'unknown')
            
            # Check if entry needs re-fetch
            if status in ('corrupted', 'expired', 'failed'):
                needs_refetch.add(dt)
            elif status == 'complete':
                # Check if expired based on TTL
                if self._is_entry_expired(entry_data):
                    needs_refetch.add(dt)
                # Check integrity if verify_checksums enabled
                elif self.verify_checksums and not self._verify_entry_integrity(entry_data):
                    needs_refetch.add(dt)
        
        # Return dates not cached or needing re-fetch
        missing = [dt for dt in all_dates if dt not in cached or dt in needs_refetch]
        return sorted(missing)
    
    def _is_entry_expired(self, entry_data: Dict) -> bool:
        """Check if a cache entry has expired based on TTL."""
        updated_at = entry_data.get('updated_at') or entry_data.get('fetched_at')
        if not updated_at:
            return True
        
        try:
            fetched_time = datetime.fromisoformat(updated_at.rstrip('Z'))
            expiry = fetched_time + timedelta(days=self.ttl_days)
            return datetime.utcnow() > expiry
        except (ValueError, TypeError):
            return True
    
    def _verify_entry_integrity(self, entry_data: Dict) -> bool:
        """Verify that a cache entry file exists and has valid checksum."""
        file_path = entry_data.get('file_path')
        if not file_path:
            return False
        
        full_path = self.cache_dir / file_path
        
        if not full_path.exists():
            return False
        
        # Check file size matches
        expected_size = entry_data.get('size_bytes')
        if expected_size is not None:
            if full_path.stat().st_size != expected_size:
                return False
        
        # Check checksum
        expected_checksum = entry_data.get('checksum_sha256')
        if expected_checksum:
            actual = compute_sha256(full_path)
            if actual != expected_checksum:
                return False
        
        return True
    
    def save_processed(
        self,
        df: pd.DataFrame,
        source: str,
        msg_type: str,
        date_val: date,
        config_hash: Optional[str] = None,
        source_file_count: Optional[int] = None,
        s3_etags: Optional[List[str]] = None,
    ) -> Path:
        """
        Save processed DataFrame to Parquet cache.
        
        Uses atomic write: temp file → rename.
        
        Args:
            df: DataFrame to cache
            source: Data source
            msg_type: Message type
            date_val: Date of the data
            config_hash: Optional hash of config used for processing
            source_file_count: Number of source files processed
            s3_etags: ETags of source S3 files
        
        Returns:
            Path to saved parquet file
        """
        # Validate inputs
        validate_source_and_type(source, msg_type)
        
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
                
                # Get file stats
                file_size = cache_path.stat().st_size
                checksum = compute_sha256(cache_path)
                
                # Update manifest with extended metadata
                with self._manifest_lock:
                    manifest = self._load_manifest()
                    manifest['entries'][key] = {
                        'status': 'complete',
                        'file_path': str(cache_path.relative_to(self.cache_dir)),
                        'checksum_sha256': checksum,
                        'size_bytes': file_size,
                        'record_count': len(df),
                        'updated_at': datetime.utcnow().isoformat(),
                        'accessed_at': datetime.utcnow().isoformat(),
                        'schema_version': self.schema_version,
                    }
                    
                    # Add optional fields
                    if config_hash is not None:
                        manifest['entries'][key]['config_hash'] = config_hash
                    if source_file_count is not None:
                        manifest['entries'][key]['source_file_count'] = source_file_count
                    if s3_etags is not None:
                        manifest['entries'][key]['s3_etags'] = s3_etags
                    
                    # Remove from in_progress if present
                    manifest.get('in_progress', {}).pop(key, None)
                    
                    self._save_manifest(manifest)
                
                logger.debug(f"Cached {len(df)} records for {key}")
                return cache_path
                
            except Exception:
                temp_path.unlink(missing_ok=True)
                raise
    
    def mark_no_data(
        self,
        source: str,
        msg_type: str,
        date_val: date,
        s3_files_found: int = 0,
    ) -> None:
        """
        Mark a date as having no data in S3.
        
        This prevents re-fetching empty dates.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date with no data
            s3_files_found: Number of S3 files found (should be 0)
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
            manifest['entries'][key] = {
                'status': 'no_data',
                'checked_at': datetime.utcnow().isoformat(),
                's3_files_found': s3_files_found,
            }
            self._save_manifest(manifest)
        
        logger.debug(f"Marked {key} as no_data")
    
    def mark_downloading(
        self,
        source: str,
        msg_type: str,
        date_val: date,
        files_total: int = 0,
    ) -> None:
        """
        Mark a date as currently downloading.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date being downloaded
            files_total: Total number of files to download
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
            manifest.setdefault('in_progress', {})[key] = {
                'status': 'downloading',
                'started_at': datetime.utcnow().isoformat(),
                'files_completed': 0,
                'files_total': files_total,
                'bytes_downloaded': 0,
            }
            self._save_manifest(manifest)
        
        logger.debug(f"Marked {key} as downloading")
    
    def update_download_progress(
        self,
        source: str,
        msg_type: str,
        date_val: date,
        files_completed: int,
        bytes_downloaded: int,
    ) -> None:
        """
        Update download progress for a date.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date being downloaded
            files_completed: Number of files completed
            bytes_downloaded: Total bytes downloaded so far
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
            if key in manifest.get('in_progress', {}):
                manifest['in_progress'][key].update({
                    'files_completed': files_completed,
                    'bytes_downloaded': bytes_downloaded,
                })
                self._save_manifest(manifest)
    
    def mark_corrupted(self, source: str, msg_type: str, date_val: date) -> None:
        """
        Mark a cache entry as corrupted.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date of corrupted entry
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
            if key in manifest.get('entries', {}):
                manifest['entries'][key]['status'] = 'corrupted'
                manifest['entries'][key]['corrupted_at'] = datetime.utcnow().isoformat()
                self._save_manifest(manifest)
        
        logger.debug(f"Marked {key} as corrupted")
    
    def mark_expired(self, source: str, msg_type: str, date_val: date) -> None:
        """
        Mark a cache entry as expired.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date of expired entry
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
            if key in manifest.get('entries', {}):
                manifest['entries'][key]['status'] = 'expired'
                self._save_manifest(manifest)
        
        logger.debug(f"Marked {key} as expired")
    
    def get_entry(
        self,
        source: str,
        msg_type: str,
        date_val: date
    ) -> Optional[CacheEntry]:
        """
        Get a specific cache entry.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date
            
        Returns:
            CacheEntry if exists, None otherwise
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        entry_data = manifest.get('entries', {}).get(key)
        if entry_data is None:
            return None
        
        return CacheEntry(entry_data)
    
    def load_parquet(
        self,
        source: str,
        msg_type: str,
        date_val: date,
        update_accessed: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Load a single cached parquet file.
        
        Args:
            source: Data source
            msg_type: Message type
            date_val: Date to load
            update_accessed: Whether to update last_accessed timestamp
            
        Returns:
            DataFrame if cached and valid, None otherwise
        """
        validate_source_and_type(source, msg_type)
        key = generate_cache_key(source, msg_type, date_val)
        
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        entry_data = manifest.get('entries', {}).get(key)
        if entry_data is None:
            return None
        
        status = entry_data.get('status')
        
        if status == 'no_data':
            return pd.DataFrame()
        
        if status != 'complete':
            return None
        
        # Check integrity
        if self.verify_checksums and not self._verify_entry_integrity(entry_data):
            self.mark_corrupted(source, msg_type, date_val)
            return None
        
        # Check expiration
        if self._is_entry_expired(entry_data):
            self.mark_expired(source, msg_type, date_val)
            return None
        
        # Load the file
        file_path = entry_data.get('file_path')
        if not file_path:
            return None
        
        full_path = self.cache_dir / file_path
        try:
            df = pd.read_parquet(full_path)
            
            if update_accessed:
                self._update_access_time(key)
            
            return df
        except Exception as e:
            logger.error(f"Failed to load {key}: {e}")
            self.mark_corrupted(source, msg_type, date_val)
            return None
    
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
    
    def evict_lru(self, bytes_needed: int) -> int:
        """
        Evict least-recently-used entries to free space.
        
        Args:
            bytes_needed: Minimum bytes to free
            
        Returns:
            Number of bytes freed
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
            
            return bytes_freed
    
    def clear_cache(
        self,
        source: Optional[str] = None,
        msg_type: Optional[str] = None
    ) -> int:
        """
        Clear cache entries.
        
        Args:
            source: If specified, only clear this source
            msg_type: If specified, only clear this message type
            
        Returns:
            Number of entries cleared
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
            
            return len(keys_to_delete)
    
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
        in_progress = manifest.get('in_progress', {})
        
        # Count by status
        status_counts = {}
        for entry in entries.values():
            status = entry.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        complete = status_counts.get('complete', 0)
        no_data = status_counts.get('no_data', 0)
        total_size = sum(e.get('size_bytes', 0) for e in entries.values())
        total_records = sum(e.get('record_count', 0) for e in entries.values())
        
        # Count by source
        source_counts = {}
        for key in entries:
            try:
                source, _, _ = parse_cache_key(key)
                source_counts[source] = source_counts.get(source, 0) + 1
            except ValueError:
                pass
        
        return {
            'total_entries': len(entries),
            'in_progress': len(in_progress),
            'complete_entries': complete,
            'no_data_entries': no_data,
            'status_counts': status_counts,
            'source_counts': source_counts,
            'total_size_bytes': total_size,
            'total_size_gb': round(total_size / (1024 ** 3), 2),
            'total_records': total_records,
            'max_size_gb': self.max_size_gb,
            'usage_percent': round((total_size / self.max_size_bytes) * 100, 1) if self.max_size_bytes else 0,
            'cache_dir': str(self.cache_dir),
        }
    
    # Alias for compatibility
    get_cache_stats = get_stats
    
    def verify_all(self) -> Dict[str, List[str]]:
        """
        Verify integrity of all cached files.
        
        Returns:
            Dictionary with lists of valid, corrupted, and missing keys
        """
        with self._manifest_lock:
            manifest = self._load_manifest()
        
        entries = manifest.get('entries', {})
        
        valid = []
        corrupted = []
        missing = []
        
        for key, entry_data in entries.items():
            if entry_data.get('status') != 'complete':
                continue
            
            file_path = entry_data.get('file_path')
            if file_path is None:
                missing.append(key)
                continue
            
            full_path = self.cache_dir / file_path
            if not full_path.exists():
                missing.append(key)
                continue
            
            if not self._verify_entry_integrity(entry_data):
                corrupted.append(key)
            else:
                valid.append(key)
        
        return {
            'valid': valid,
            'corrupted': corrupted,
            'missing': missing,
        }
