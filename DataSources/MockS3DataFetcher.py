"""
MockS3DataFetcher - Local file-based mock for S3DataFetcher.

Allows testing the full DataSources pipeline without AWS credentials
by reading from local synthetic data instead of S3.

Usage:
    from DataSources.MockS3DataFetcher import MockS3DataFetcher
    from DataSources.config import DataSourceConfig, DateRangeConfig
    
    config = DataSourceConfig(
        source="wydot",
        message_type="BSM",
        date_range=DateRangeConfig(start_date=date(2021, 4, 1))
    )
    
    fetcher = MockS3DataFetcher(config, data_dir=Path("test_data/synthetic"))
    objects = fetcher.list_objects_for_date(date(2021, 4, 1))
    df = fetcher.fetch_and_parse()
"""

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Any
from dataclasses import dataclass

from .config import DataSourceConfig, validate_source_and_type, generate_cache_key
from .S3DataFetcher import S3Object, DownloadResult, RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class MockS3Object:
    """Mock S3 object representing a local file."""
    key: str
    size: int
    etag: str
    local_path: Path
    last_modified: Optional[str] = None
    
    @property
    def filename(self) -> str:
        return self.key.split('/')[-1]
    
    def to_s3_object(self) -> S3Object:
        """Convert to S3Object for compatibility."""
        return S3Object(
            key=self.key,
            size=self.size,
            etag=self.etag,
            last_modified=self.last_modified
        )


class MockS3DataFetcher:
    """
    Mock S3DataFetcher that reads from local files.
    
    Maintains the same interface as S3DataFetcher but reads from
    a local directory structure matching S3 layout.
    """
    
    def __init__(
        self,
        config: DataSourceConfig,
        data_dir: Path,
        simulate_latency: bool = False,
        latency_ms: int = 50,
    ):
        """
        Initialize MockS3DataFetcher.
        
        Args:
            config: DataSourceConfig with source, message_type, date_range
            data_dir: Local directory with S3-like structure
            simulate_latency: Add artificial delays to simulate network
            latency_ms: Latency in milliseconds when simulate_latency=True
        """
        self.config = config
        self.data_dir = Path(data_dir)
        self.source = config.source
        self.message_type = config.message_type
        self.simulate_latency = simulate_latency
        self.latency_ms = latency_ms
        
        # Validate source and type
        validate_source_and_type(self.source, self.message_type)
        
        # Rate limiter (for consistent interface, though not really needed locally)
        self._rate_limiter = RateLimiter(max_requests_per_second=100)
        
        # Verify data directory exists
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Mock data directory not found: {self.data_dir}")
        
        logger.info(f"Initialized MockS3DataFetcher for {self.source}/{self.message_type} from {self.data_dir}")
    
    def _simulate_delay(self):
        """Add artificial latency if configured."""
        if self.simulate_latency:
            time.sleep(self.latency_ms / 1000)
    
    def _build_local_prefix(self, dt: date) -> Path:
        """Build local path for a specific date."""
        return self.data_dir / self.source / self.message_type / str(dt.year) / f"{dt.month:02d}" / f"{dt.day:02d}"
    
    def _build_s3_key(self, local_path: Path) -> str:
        """Convert local path to S3-like key."""
        # Get path relative to data_dir
        rel_path = local_path.relative_to(self.data_dir)
        return str(rel_path)
    
    def _compute_etag(self, filepath: Path) -> str:
        """Compute a mock ETag for a file."""
        import hashlib
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return f'"{hasher.hexdigest()}"'
    
    def list_objects_for_date(self, target_date: date) -> List[MockS3Object]:
        """
        List all objects for a specific date.
        
        Args:
            target_date: Date to list objects for
        
        Returns:
            List of MockS3Object instances
        """
        self._simulate_delay()
        
        date_dir = self._build_local_prefix(target_date)
        
        if not date_dir.exists():
            logger.debug(f"No data directory for {target_date}: {date_dir}")
            return []
        
        objects = []
        
        # Iterate through hour directories
        for hour_dir in sorted(date_dir.iterdir()):
            if not hour_dir.is_dir():
                continue
            
            for file_path in sorted(hour_dir.glob("*.json")):
                if file_path.is_file():
                    key = self._build_s3_key(file_path)
                    size = file_path.stat().st_size
                    etag = self._compute_etag(file_path)
                    
                    objects.append(MockS3Object(
                        key=key,
                        size=size,
                        etag=etag,
                        local_path=file_path
                    ))
        
        logger.debug(f"Found {len(objects)} objects for {target_date}")
        return objects
    
    def list_objects_for_date_range(
        self,
        start_date: date,
        end_date: date
    ) -> Iterator[MockS3Object]:
        """
        List all objects for a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
        
        Yields:
            MockS3Object instances
        """
        current = start_date
        while current <= end_date:
            for obj in self.list_objects_for_date(current):
                yield obj
            current += timedelta(days=1)
    
    def download_object(self, obj: MockS3Object, dest_path: Path) -> DownloadResult:
        """
        "Download" (copy) an object to destination.
        
        Args:
            obj: MockS3Object to download
            dest_path: Destination path
        
        Returns:
            DownloadResult with success/failure info
        """
        self._simulate_delay()
        
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            import shutil
            shutil.copy2(obj.local_path, dest_path)
            
            return DownloadResult(
                s3_obj=obj.to_s3_object(),
                local_path=dest_path,
                success=True,
                bytes_downloaded=obj.size
            )
        except Exception as e:
            logger.error(f"Failed to copy {obj.key}: {e}")
            return DownloadResult(
                s3_obj=obj.to_s3_object(),
                local_path=None,
                success=False,
                error=str(e)
            )
    
    def read_object(self, obj: MockS3Object) -> bytes:
        """
        Read object content directly.
        
        Args:
            obj: MockS3Object to read
        
        Returns:
            Object content as bytes
        """
        self._simulate_delay()
        return obj.local_path.read_bytes()
    
    def read_object_json(self, obj: MockS3Object) -> List[Dict[str, Any]]:
        """
        Read and parse JSON/NDJSON object.
        
        Args:
            obj: MockS3Object to read
        
        Returns:
            List of parsed JSON records
        """
        content = self.read_object(obj).decode('utf-8')
        
        records = []
        for line in content.strip().split('\n'):
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse line in {obj.key}: {e}")
        
        return records
    
    def fetch_all_records(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch all records for date range.
        
        Uses config date range if not specified.
        
        Args:
            start_date: Override start date
            end_date: Override end date
        
        Yields:
            Parsed BSM records
        """
        if start_date is None or end_date is None:
            config_start, config_end = self.config.date_range.get_effective_dates()
            start_date = start_date or config_start
            end_date = end_date or config_end
        
        logger.info(f"Fetching records from {start_date} to {end_date}")
        
        record_count = 0
        for obj in self.list_objects_for_date_range(start_date, end_date):
            for record in self.read_object_json(obj):
                record_count += 1
                yield record
        
        logger.info(f"Fetched {record_count} total records")
    
    def get_date_stats(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Get statistics about available data.
        
        Returns:
            Dict with counts and sizes
        """
        if start_date is None or end_date is None:
            config_start, config_end = self.config.date_range.get_effective_dates()
            start_date = start_date or config_start
            end_date = end_date or config_end
        
        stats = {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "source": self.source,
            "message_type": self.message_type,
            "total_files": 0,
            "total_bytes": 0,
            "by_date": {}
        }
        
        current = start_date
        while current <= end_date:
            objects = self.list_objects_for_date(current)
            day_bytes = sum(obj.size for obj in objects)
            stats["by_date"][str(current)] = {
                "files": len(objects),
                "bytes": day_bytes
            }
            stats["total_files"] += len(objects)
            stats["total_bytes"] += day_bytes
            current += timedelta(days=1)
        
        return stats


def create_mock_fetcher(
    config: DataSourceConfig,
    data_dir: Path,
    create_data: bool = True,
    records_per_hour: int = 100,
) -> MockS3DataFetcher:
    """
    Create a MockS3DataFetcher, optionally generating test data.
    
    Args:
        config: DataSourceConfig
        data_dir: Directory for mock data
        create_data: Whether to generate synthetic data if not exists
        records_per_hour: Records per hour when generating
    
    Returns:
        Configured MockS3DataFetcher
    """
    from .SyntheticDataGenerator import SyntheticBSMGenerator
    
    # Generate data if needed
    start_date, end_date = config.date_range.get_effective_dates()
    
    if create_data:
        generator = SyntheticBSMGenerator(seed=42)
        
        current = start_date
        while current <= end_date:
            day_dir = data_dir / config.source / config.message_type / str(current.year) / f"{current.month:02d}" / f"{current.day:02d}"
            
            if not day_dir.exists() or not any(day_dir.glob("*/*.json")):
                logger.info(f"Generating synthetic data for {current}")
                generator.generate_day_structure(
                    data_dir,
                    current,
                    source=config.source,
                    message_type=config.message_type,
                    records_per_hour=records_per_hour
                )
            
            current += timedelta(days=1)
    
    return MockS3DataFetcher(config, data_dir)
