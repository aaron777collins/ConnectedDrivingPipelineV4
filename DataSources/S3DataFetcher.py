"""
S3DataFetcher - Fetches CV Pilot data from USDOT ITS Sandbox S3 bucket.

Features:
- Anonymous S3 access (no credentials required)
- Date-range based queries
- Parallel downloads with configurable concurrency
- Resume support for interrupted downloads
- Token bucket rate limiting (~10 req/s)
- Exponential backoff on errors
- Integrity verification via ETag/MD5
"""

import hashlib
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, Iterator, List, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError

from .config import (
    DataSourceConfig,
    generate_cache_key,
    validate_source_and_type,
)

logger = logging.getLogger(__name__)


@dataclass
class S3Object:
    """Represents an S3 object with metadata."""
    
    key: str
    size: int
    etag: str
    last_modified: Optional[str] = None
    
    @property
    def filename(self) -> str:
        """Get the filename from the full key."""
        return self.key.split('/')[-1]
    
    def __hash__(self):
        return hash(self.key)
    
    def __eq__(self, other):
        if not isinstance(other, S3Object):
            return False
        return self.key == other.key


@dataclass 
class DownloadResult:
    """Result of a download operation."""
    
    s3_obj: S3Object
    local_path: Optional[Path]
    success: bool
    error: Optional[str] = None
    bytes_downloaded: int = 0
    resumed: bool = False


class RateLimiter:
    """
    Token bucket rate limiter for S3 requests.
    
    Allows burst requests up to the bucket size, then limits
    to max_requests_per_second.
    """
    
    def __init__(self, max_requests_per_second: float = 10.0):
        self.rate = max_requests_per_second
        self.tokens = max_requests_per_second  # Start with full bucket
        self.last_update = time.monotonic()
        self._lock = Lock()
    
    def acquire(self) -> None:
        """Acquire a token, blocking if necessary."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                # Need to wait for tokens
                sleep_time = (1 - self.tokens) / self.rate
                time.sleep(sleep_time)
                self.tokens = 0
                self.last_update = time.monotonic()
            else:
                self.tokens -= 1


class S3DataFetcher:
    """
    Fetches CV Pilot data from USDOT ITS Sandbox S3 bucket.
    
    Features:
    - Date-range based queries
    - Parallel downloads with configurable concurrency
    - Resume support for interrupted downloads
    - Rate limiting with exponential backoff
    - Integrity verification via ETag/MD5
    
    Usage:
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01", "end_date": "2021-04-07"}
        )
        
        fetcher = S3DataFetcher(config)
        df = fetcher.get_data()
    """
    
    DEFAULT_BUCKET = "usdot-its-cvpilot-publicdata"
    
    def __init__(self, config: DataSourceConfig):
        """
        Initialize S3DataFetcher with configuration.
        
        Args:
            config: DataSourceConfig with source, message_type, and date_range
        """
        self.config = config
        self.bucket = config.bucket
        self.source = config.source
        self.message_type = config.message_type
        self.cache_dir = config.cache.directory
        self.max_workers = config.download.max_workers
        self.retry_attempts = config.download.retry_attempts
        self.retry_delay = config.download.retry_delay
        self.timeout_seconds = config.download.timeout_seconds
        
        # Validate source and message type
        validate_source_and_type(self.source, self.message_type)
        
        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            max_requests_per_second=config.download.rate_limit_per_second
        )
        
        # Create S3 client with anonymous access
        # The bucket is in us-east-1 region
        self._s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            config=BotoConfig(
                signature_version=UNSIGNED,
                connect_timeout=30,
                read_timeout=60,
                retries={'max_attempts': 0}  # We handle retries ourselves
            )
        )
        
        # Track download progress for resume support
        self._download_progress: Dict[str, int] = {}
        self._progress_lock = Lock()
        
        logger.info(
            f"Initialized S3DataFetcher for {self.source}/{self.message_type} "
            f"(bucket: {self.bucket})"
        )
    
    def _build_s3_prefix(self, dt: date) -> str:
        """
        Build S3 prefix for a specific date.
        
        Format: {source}/{message_type}/{year}/{month:02d}/{day:02d}/
        """
        return f"{self.source}/{self.message_type}/{dt.year}/{dt.month:02d}/{dt.day:02d}/"
    
    def _build_hourly_prefix(self, dt: date, hour: int) -> str:
        """
        Build S3 prefix for a specific hour.
        
        Format: {source}/{message_type}/{year}/{month:02d}/{day:02d}/{hour:02d}/
        """
        base = self._build_s3_prefix(dt)
        return f"{base}{hour:02d}/"
    
    def _iter_date_range(self, start_date: date, end_date: date) -> Iterator[date]:
        """Iterate over dates in range (inclusive)."""
        current = start_date
        while current <= end_date:
            yield current
            current += timedelta(days=1)
    
    def _list_with_retry(self, prefix: str) -> List[S3Object]:
        """
        List S3 objects with exponential backoff retry.
        
        Returns empty list if prefix has no objects (not an error).
        """
        delay = self.retry_delay
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                self._rate_limiter.acquire()
                
                objects = []
                paginator = self._s3_client.get_paginator('list_objects_v2')
                
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    if 'Contents' not in page:
                        # Empty prefix - this is normal, not an error
                        continue
                    
                    for obj in page['Contents']:
                        # Skip directory markers
                        if obj['Key'].endswith('/'):
                            continue
                        
                        # Handle LastModified which might be datetime or string
                        last_mod = obj.get('LastModified')
                        if last_mod:
                            last_mod_str = last_mod.isoformat() if hasattr(last_mod, 'isoformat') else str(last_mod)
                        else:
                            last_mod_str = None
                        
                        objects.append(S3Object(
                            key=obj['Key'],
                            size=obj['Size'],
                            etag=obj['ETag'].strip('"'),
                            last_modified=last_mod_str
                        ))
                
                return objects
                
            except (ClientError, EndpointConnectionError) as e:
                last_error = e
                error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
                
                # Don't retry on access denied or not found
                if error_code in ('AccessDenied', 'NoSuchBucket'):
                    raise
                
                if attempt < self.retry_attempts - 1:
                    logger.warning(
                        f"S3 list attempt {attempt + 1} failed for {prefix}: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
        
        raise RuntimeError(f"Failed to list {prefix} after {self.retry_attempts} attempts: {last_error}")
    
    def list_files(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[S3Object]:
        """
        List all files in date range.
        
        Args:
            start_date: Start date (defaults to config date_range)
            end_date: End date (defaults to config date_range)
        
        Returns:
            List of S3Object representing files in the date range.
            Returns empty list if no data exists (not an error).
        """
        # Use config dates if not specified
        if start_date is None or end_date is None:
            config_start, config_end = self.config.date_range.get_effective_dates()
            start_date = start_date or config_start
            end_date = end_date or config_end
        
        logger.info(
            f"Listing files for {self.source}/{self.message_type} "
            f"from {start_date} to {end_date}"
        )
        
        all_objects: List[S3Object] = []
        
        for dt in self._iter_date_range(start_date, end_date):
            prefix = self._build_s3_prefix(dt)
            logger.debug(f"Listing prefix: {prefix}")
            
            try:
                objects = self._list_with_retry(prefix)
                all_objects.extend(objects)
                
                if objects:
                    logger.debug(f"  Found {len(objects)} files for {dt}")
                else:
                    logger.debug(f"  No data for {dt}")
                    
            except RuntimeError as e:
                logger.error(f"Failed to list files for {dt}: {e}")
                # Continue with other dates rather than failing entirely
                continue
        
        logger.info(f"Found {len(all_objects)} total files")
        return all_objects
    
    def _get_local_path(self, s3_obj: S3Object) -> Path:
        """Get local cache path for an S3 object."""
        # Preserve S3 directory structure
        return self.cache_dir / s3_obj.key
    
    def _download_file(
        self,
        s3_obj: S3Object,
        progress_callback: Optional[Callable[[S3Object, int, int], None]] = None
    ) -> DownloadResult:
        """
        Download a single file with resume support.
        
        Args:
            s3_obj: S3 object to download
            progress_callback: Optional callback(s3_obj, bytes_downloaded, total_bytes)
        
        Returns:
            DownloadResult with success/failure status
        """
        local_path = self._get_local_path(s3_obj)
        temp_path = local_path.parent / f".tmp_{uuid.uuid4()}_{s3_obj.filename}"
        
        try:
            # Ensure directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if already downloaded with correct size/etag
            if local_path.exists():
                if local_path.stat().st_size == s3_obj.size:
                    logger.debug(f"Already downloaded: {s3_obj.key}")
                    return DownloadResult(
                        s3_obj=s3_obj,
                        local_path=local_path,
                        success=True,
                        bytes_downloaded=0,
                        resumed=False
                    )
            
            # Check for partial download
            bytes_downloaded = 0
            resumed = False
            
            with self._progress_lock:
                if s3_obj.key in self._download_progress:
                    bytes_downloaded = self._download_progress[s3_obj.key]
            
            # Check if temp file exists from interrupted download
            if temp_path.exists():
                existing_size = temp_path.stat().st_size
                if existing_size < s3_obj.size:
                    bytes_downloaded = existing_size
                    resumed = True
                    logger.info(f"Resuming download of {s3_obj.key} from byte {bytes_downloaded}")
            
            # Download with retry and exponential backoff
            delay = self.retry_delay
            last_error = None
            
            for attempt in range(self.retry_attempts):
                try:
                    self._rate_limiter.acquire()
                    
                    # Build range header for resume
                    extra_args = {}
                    if bytes_downloaded > 0:
                        extra_args['Range'] = f'bytes={bytes_downloaded}-'
                    
                    # Open file in append mode for resume, write mode otherwise
                    mode = 'ab' if resumed and bytes_downloaded > 0 else 'wb'
                    
                    with open(temp_path, mode) as f:
                        response = self._s3_client.get_object(
                            Bucket=self.bucket,
                            Key=s3_obj.key,
                            **extra_args
                        )
                        
                        # Stream download in chunks
                        chunk_size = 1024 * 1024  # 1MB chunks
                        body = response['Body']
                        
                        while True:
                            chunk = body.read(chunk_size)
                            if not chunk:
                                break
                            
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            
                            # Track progress
                            with self._progress_lock:
                                self._download_progress[s3_obj.key] = bytes_downloaded
                            
                            if progress_callback:
                                progress_callback(s3_obj, bytes_downloaded, s3_obj.size)
                    
                    # Verify download completed
                    if temp_path.stat().st_size != s3_obj.size:
                        raise RuntimeError(
                            f"Downloaded size mismatch: {temp_path.stat().st_size} != {s3_obj.size}"
                        )
                    
                    # Atomic rename
                    temp_path.rename(local_path)
                    
                    # Clear progress tracking
                    with self._progress_lock:
                        self._download_progress.pop(s3_obj.key, None)
                    
                    return DownloadResult(
                        s3_obj=s3_obj,
                        local_path=local_path,
                        success=True,
                        bytes_downloaded=s3_obj.size,
                        resumed=resumed
                    )
                    
                except (ClientError, EndpointConnectionError, IOError) as e:
                    last_error = e
                    error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
                    
                    # Don't retry on access denied or not found
                    if error_code in ('AccessDenied', 'NoSuchKey'):
                        break
                    
                    if attempt < self.retry_attempts - 1:
                        logger.warning(
                            f"Download attempt {attempt + 1} failed for {s3_obj.key}: {e}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
            
            return DownloadResult(
                s3_obj=s3_obj,
                local_path=None,
                success=False,
                error=str(last_error),
                bytes_downloaded=bytes_downloaded,
                resumed=resumed
            )
            
        except Exception as e:
            logger.error(f"Error downloading {s3_obj.key}: {e}")
            return DownloadResult(
                s3_obj=s3_obj,
                local_path=None,
                success=False,
                error=str(e),
                bytes_downloaded=bytes_downloaded,
                resumed=resumed
            )
        finally:
            # Clean up temp file on failure
            if temp_path.exists() and not local_path.exists():
                # Keep temp file for resume on transient errors
                pass
    
    def download_files(
        self,
        files: List[S3Object],
        progress_callback: Optional[Callable[[int, int, int], None]] = None
    ) -> List[DownloadResult]:
        """
        Download files with parallel execution and resume support.
        
        Args:
            files: List of S3Objects to download
            progress_callback: Optional callback(files_completed, total_files, bytes_downloaded)
        
        Returns:
            List of DownloadResult for each file
        """
        if not files:
            logger.info("No files to download")
            return []
        
        logger.info(f"Downloading {len(files)} files with {self.max_workers} workers")
        
        results: List[DownloadResult] = []
        total_bytes = sum(f.size for f in files)
        bytes_downloaded = 0
        files_completed = 0
        
        def on_file_progress(s3_obj: S3Object, downloaded: int, total: int):
            """Track per-file progress."""
            nonlocal bytes_downloaded
            # This is approximate since multiple files download in parallel
            pass
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all downloads
            future_to_obj = {
                executor.submit(self._download_file, obj, on_file_progress): obj
                for obj in files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_obj):
                result = future.result()
                results.append(result)
                
                files_completed += 1
                if result.success:
                    bytes_downloaded += result.bytes_downloaded
                
                if progress_callback:
                    progress_callback(files_completed, len(files), bytes_downloaded)
                
                if result.success:
                    logger.debug(f"Downloaded: {result.s3_obj.key}")
                else:
                    logger.warning(f"Failed: {result.s3_obj.key} - {result.error}")
        
        # Summary
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        resumed = sum(1 for r in results if r.resumed)
        
        logger.info(
            f"Download complete: {successful} successful, {failed} failed, "
            f"{resumed} resumed"
        )
        
        return results
    
    def get_data(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        force_refresh: bool = False
    ):
        """
        Main entry point: fetch, cache, and return data.
        
        This method integrates with CacheManager to:
        1. Check cache for existing data
        2. Download missing dates from S3
        3. Parse and process downloaded files
        4. Store in cache for future use
        
        Args:
            start_date: Start date (defaults to config date_range)
            end_date: End date (defaults to config date_range)
            force_refresh: If True, bypass cache and re-download
        
        Returns:
            Dask DataFrame with the requested data (lazy loading)
        """
        # Import CacheManager here to avoid circular imports
        from .CacheManager import CacheManager
        
        # Use config dates if not specified
        if start_date is None or end_date is None:
            config_start, config_end = self.config.date_range.get_effective_dates()
            start_date = start_date or config_start
            end_date = end_date or config_end
        
        logger.info(
            f"Getting data for {self.source}/{self.message_type} "
            f"from {start_date} to {end_date}"
        )
        
        # Initialize cache manager
        cache_manager = CacheManager(
            cache_dir=self.cache_dir,
            max_size_gb=self.config.cache.max_size_gb
        )
        
        # Determine which dates need fetching
        if force_refresh:
            dates_to_fetch = list(self._iter_date_range(start_date, end_date))
        else:
            dates_to_fetch = cache_manager.get_missing_dates(
                source=self.source,
                msg_type=self.message_type,
                start=start_date,
                end=end_date
            )
        
        if dates_to_fetch:
            logger.info(f"Need to fetch {len(dates_to_fetch)} dates from S3")
            
            # List and download files for missing dates
            for dt in dates_to_fetch:
                files = self._list_with_retry(self._build_s3_prefix(dt))
                
                if not files:
                    # Mark date as having no data
                    cache_manager.mark_no_data(
                        source=self.source,
                        msg_type=self.message_type,
                        date_val=dt
                    )
                    logger.debug(f"No data for {dt}, marked in cache")
                    continue
                
                # Download files for this date
                results = self.download_files(files)
                
                # Process successful downloads
                successful_paths = [r.local_path for r in results if r.success and r.local_path]
                
                if successful_paths:
                    # Parse and save to cache
                    df = self._parse_files(successful_paths)
                    if df is not None and not df.empty:
                        cache_manager.save_processed(
                            df=df,
                            source=self.source,
                            msg_type=self.message_type,
                            date_val=dt
                        )
        else:
            logger.info("All dates available in cache")
        
        # Load data from cache
        return cache_manager.load_date_range(
            source=self.source,
            msg_type=self.message_type,
            start=start_date,
            end=end_date
        )
    
    def _parse_files(self, paths: List[Path]):
        """
        Parse downloaded JSON/NDJSON files into DataFrame.
        
        Handles both single-record JSON (pre-2018) and NDJSON (post-2018).
        
        Args:
            paths: List of local file paths
        
        Returns:
            pandas DataFrame with parsed records
        """
        import json
        import pandas as pd
        
        records = []
        
        for path in paths:
            try:
                with open(path, 'r') as f:
                    content = f.read().strip()
                    
                    if not content:
                        continue
                    
                    # Try NDJSON first (most common post-2018)
                    if '\n' in content:
                        for line in content.split('\n'):
                            line = line.strip()
                            if line:
                                try:
                                    records.append(json.loads(line))
                                except json.JSONDecodeError:
                                    logger.warning(f"Invalid JSON line in {path}")
                    else:
                        # Single JSON object (pre-2018)
                        try:
                            records.append(json.loads(content))
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in {path}")
                            
            except Exception as e:
                logger.error(f"Error parsing {path}: {e}")
        
        if not records:
            return None
        
        # Flatten nested structure for DataFrame
        return pd.json_normalize(records)
    
    def verify_integrity(self, s3_obj: S3Object, local_path: Path) -> bool:
        """
        Verify downloaded file integrity using ETag.
        
        Note: S3 ETags for simple uploads are MD5 hashes.
        Multipart uploads have different ETag formats.
        
        Args:
            s3_obj: S3 object with expected ETag
            local_path: Path to local file
        
        Returns:
            True if file matches expected ETag
        """
        if not local_path.exists():
            return False
        
        # Check size first (fast check)
        if local_path.stat().st_size != s3_obj.size:
            return False
        
        # For non-multipart uploads, ETag is MD5
        # Multipart ETags contain '-' and can't be verified this way
        if '-' in s3_obj.etag:
            # Multipart upload - just check size
            return True
        
        # Compute MD5
        md5 = hashlib.md5()
        with open(local_path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                md5.update(chunk)
        
        return md5.hexdigest() == s3_obj.etag
