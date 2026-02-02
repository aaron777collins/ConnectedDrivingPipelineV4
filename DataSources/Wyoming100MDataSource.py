"""
Wyoming100MDataSource - Data source for 100M row Wyoming CV Pilot dataset.

This data source is designed to handle the large-scale Wyoming CV Pilot dataset
with 100 million rows. It supports:
- Downloading from S3 (when credentials are available)
- Synthetic data generation for testing
- Robust caching with parquet format
- Resume capability for interrupted downloads

Usage:
    # With S3 credentials
    source = Wyoming100MDataSource(use_synthetic=False)
    df = source.get_data()
    
    # With synthetic data (for testing)
    source = Wyoming100MDataSource(use_synthetic=True, num_rows=100_000_000)
    df = source.get_data()
"""

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd

try:
    import dask.dataframe as dd
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

from .config import DataSourceConfig, VALID_SOURCES
from .SyntheticDataGenerator import SyntheticBSMGenerator
from .CacheManager import CacheManager

logger = logging.getLogger(__name__)


class Wyoming100MDataSource:
    """
    Data source for Wyoming CV Pilot 100M row dataset.
    
    Supports both S3 download (when credentials available) and
    synthetic data generation for testing.
    """
    
    # Default configuration for Wyoming BSM data
    DEFAULT_SOURCE = "wydot"
    DEFAULT_MESSAGE_TYPE = "BSM"
    
    # Date range that typically contains 100M+ rows
    # April 2021 was a high-traffic month for Wyoming CV Pilot
    DEFAULT_START_DATE = date(2021, 4, 1)
    DEFAULT_END_DATE = date(2021, 4, 30)
    
    # Cache directory
    DEFAULT_CACHE_DIR = Path("data/cache/wyoming100m")
    
    # Wyoming I-80 corridor center point (for filtering)
    DEFAULT_CENTER_LAT = 41.5430216
    DEFAULT_CENTER_LON = -106.0831353
    DEFAULT_MAX_DISTANCE_METERS = 2000
    
    def __init__(
        self,
        use_synthetic: bool = False,
        num_rows: Optional[int] = None,
        cache_dir: Optional[Path] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        center_lat: Optional[float] = None,
        center_lon: Optional[float] = None,
        max_distance_meters: Optional[float] = None,
        random_seed: int = 42,
    ):
        """
        Initialize Wyoming100M data source.
        
        Args:
            use_synthetic: If True, generate synthetic data instead of downloading
            num_rows: Number of rows to generate (only for synthetic mode)
            cache_dir: Directory for cached data
            start_date: Start date for data (defaults to 2021-04-01)
            end_date: End date for data (defaults to 2021-04-30)
            center_lat: Center latitude for spatial filtering
            center_lon: Center longitude for spatial filtering
            max_distance_meters: Maximum distance from center for filtering
            random_seed: Random seed for reproducibility
        """
        self.use_synthetic = use_synthetic
        self.num_rows = num_rows or 100_000_000
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.start_date = start_date or self.DEFAULT_START_DATE
        self.end_date = end_date or self.DEFAULT_END_DATE
        self.center_lat = center_lat or self.DEFAULT_CENTER_LAT
        self.center_lon = center_lon or self.DEFAULT_CENTER_LON
        self.max_distance_meters = max_distance_meters or self.DEFAULT_MAX_DISTANCE_METERS
        self.random_seed = random_seed
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cache manager
        self.cache_manager = CacheManager(
            cache_dir=self.cache_dir,
            max_size_gb=200.0,  # Allow large cache for 100M rows
            ttl_days=None,  # Historical data never expires
            verify_checksums=True,
        )
        
        logger.info(
            f"Initialized Wyoming100MDataSource "
            f"(synthetic={use_synthetic}, rows={num_rows}, "
            f"date_range={start_date} to {end_date})"
        )
    
    def get_cache_path(self) -> Path:
        """Get the path for the cached parquet file."""
        return self.cache_dir / "wyoming_100m.parquet"
    
    def is_cached(self) -> bool:
        """Check if data is already cached."""
        cache_path = self.get_cache_path()
        return cache_path.exists()
    
    def get_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get the Wyoming 100M dataset.
        
        Args:
            force_refresh: If True, regenerate/redownload even if cached
        
        Returns:
            DataFrame with Wyoming BSM data
        """
        cache_path = self.get_cache_path()
        
        # Check cache first
        if not force_refresh and cache_path.exists():
            logger.info(f"Loading cached data from {cache_path}")
            return pd.read_parquet(cache_path)
        
        # Generate or download data
        if self.use_synthetic:
            logger.info(f"Generating {self.num_rows:,} synthetic rows...")
            df = self._generate_synthetic_data()
        else:
            logger.info("Downloading from S3...")
            df = self._download_from_s3()
        
        # Cache the result
        logger.info(f"Caching data to {cache_path}")
        df.to_parquet(cache_path, index=False, compression='snappy')
        
        return df
    
    def get_data_dask(self, force_refresh: bool = False):
        """
        Get the Wyoming 100M dataset as a Dask DataFrame.
        
        For large datasets, use this method for lazy loading.
        
        Args:
            force_refresh: If True, regenerate/redownload even if cached
        
        Returns:
            Dask DataFrame with Wyoming BSM data
        """
        if not HAS_DASK:
            raise ImportError("Dask is required for get_data_dask(). Install with: pip install dask[dataframe]")
        
        cache_path = self.get_cache_path()
        
        # Ensure data exists
        if not cache_path.exists() or force_refresh:
            self.get_data(force_refresh=force_refresh)
        
        return dd.read_parquet(cache_path)
    
    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate synthetic BSM data matching Wyoming schema."""
        generator = SyntheticBSMGenerator(
            seed=self.random_seed,
            num_vehicles=1000,  # More vehicles for 100M rows
            schema_version=6,
        )
        
        # Calculate time span for records
        num_days = (self.end_date - self.start_date).days + 1
        records_per_day = self.num_rows // num_days
        
        logger.info(f"Generating {records_per_day:,} records per day for {num_days} days")
        
        all_records = []
        current_date = datetime.combine(self.start_date, datetime.min.time())
        
        batch_size = 100_000  # Process in batches to manage memory
        total_generated = 0
        
        while total_generated < self.num_rows:
            batch_records = []
            for _ in range(min(batch_size, self.num_rows - total_generated)):
                # Add some time variation
                timestamp = current_date + timedelta(
                    seconds=generator.rng.uniform(0, 86400)
                )
                
                bsm = generator.generate_bsm(timestamp)
                
                # Flatten to CSV-compatible format
                flat_record = self._flatten_bsm(bsm)
                batch_records.append(flat_record)
                
                total_generated += 1
                
                if total_generated % 1_000_000 == 0:
                    logger.info(f"Generated {total_generated:,} / {self.num_rows:,} records")
            
            all_records.extend(batch_records)
            
            # Move to next day periodically
            if total_generated % records_per_day == 0:
                current_date += timedelta(days=1)
                if current_date.date() > self.end_date:
                    current_date = datetime.combine(self.start_date, datetime.min.time())
        
        logger.info(f"Generated {len(all_records):,} total records")
        return pd.DataFrame(all_records)
    
    def _flatten_bsm(self, bsm: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested BSM structure to CSV-compatible format."""
        metadata = bsm.get("metadata", {})
        payload = bsm.get("payload", {})
        data = payload.get("data", {})
        core = data.get("coreData", {})
        position = core.get("position", {})
        accel = core.get("accelSet", {})
        accuracy = core.get("accuracy", {})
        brakes = core.get("brakes", {})
        wheel = brakes.get("wheelBrakes", {})
        size = core.get("size", {})
        rmd = metadata.get("receivedMessageDetails", {}).get("locationData", {})
        serial = metadata.get("serialId", {})
        
        return {
            "dataType": payload.get("dataType", ""),
            "metadata_generatedAt": metadata.get("recordGeneratedAt", ""),
            "metadata_generatedBy": metadata.get("recordGeneratedBy", ""),
            "metadata_logFileName": metadata.get("logFileName", ""),
            "metadata_schemaVersion": metadata.get("schemaVersion", 6),
            "metadata_securityResultCode": metadata.get("securityResultCode", ""),
            "metadata_sanitized": metadata.get("sanitized", True),
            "metadata_payloadType": metadata.get("payloadType", ""),
            "metadata_recordType": metadata.get("recordType", ""),
            "metadata_serialId_streamId": serial.get("streamId", ""),
            "metadata_serialId_bundleSize": serial.get("bundleSize", 0),
            "metadata_serialId_bundleId": serial.get("bundleId", 0),
            "metadata_serialId_recordId": serial.get("recordId", 0),
            "metadata_serialId_serialNumber": serial.get("serialNumber", 0),
            "metadata_receivedAt": metadata.get("odeReceivedAt", ""),
            "metadata_rmd_elevation": rmd.get("elevation", ""),
            "metadata_rmd_heading": rmd.get("heading", ""),
            "metadata_rmd_latitude": rmd.get("latitude", ""),
            "metadata_rmd_longitude": rmd.get("longitude", ""),
            "metadata_rmd_speed": rmd.get("speed", ""),
            "metadata_rmd_rxSource": rmd.get("rxSource", ""),
            "metadata_bsmSource": metadata.get("bsmSource", ""),
            "coreData_msgCnt": core.get("msgCnt", 0),
            "coreData_id": core.get("id", ""),
            "coreData_secMark": core.get("secMark", 0),
            "coreData_position_lat": position.get("latitude", 0.0),
            "coreData_position_long": position.get("longitude", 0.0),
            "coreData_elevation": position.get("elevation", 0.0),
            "coreData_accelset_accelYaw": accel.get("accelYaw", 0.0),
            "coreData_accuracy_semiMajor": accuracy.get("semiMajor", 0.0),
            "coreData_accuracy_semiMinor": accuracy.get("semiMinor", 0.0),
            "coreData_transmission": core.get("transmission", ""),
            "coreData_speed": core.get("speed", 0.0),
            "coreData_heading": core.get("heading", 0.0),
            "coreData_brakes_wheelBrakes_leftFront": wheel.get("leftFront", False),
            "coreData_brakes_wheelBrakes_rightFront": wheel.get("rightFront", False),
            "coreData_brakes_wheelBrakes_unavailable": wheel.get("unavailable", True),
            "coreData_brakes_wheelBrakes_leftRear": wheel.get("leftRear", False),
            "coreData_brakes_wheelBrakes_rightRear": wheel.get("rightRear", False),
            "coreData_brakes_traction": brakes.get("traction", "unavailable"),
            "coreData_brakes_abs": brakes.get("abs", "unavailable"),
            "coreData_brakes_scs": brakes.get("scs", "unavailable"),
            "coreData_brakes_brakeBoost": brakes.get("brakeBoost", "unavailable"),
            "coreData_brakes_auxBrakes": brakes.get("auxBrakes", "unavailable"),
            "coreData_size": f'{{"width": {size.get("width", 200)}, "length": {size.get("length", 500)}}}',
            "coreData_position": f"POINT ({position.get('longitude', 0.0)} {position.get('latitude', 0.0)})",
        }
    
    def _download_from_s3(self) -> pd.DataFrame:
        """Download data from S3 bucket."""
        # This requires AWS credentials
        try:
            from .S3DataFetcher import S3DataFetcher
            
            config = DataSourceConfig(
                source=self.DEFAULT_SOURCE,
                message_type=self.DEFAULT_MESSAGE_TYPE,
                date_range={
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                },
                cache={"directory": str(self.cache_dir)},
                download={"use_anonymous": False},  # Requires credentials
            )
            
            fetcher = S3DataFetcher(config)
            
            # List all files
            files = fetcher.list_files(self.start_date, self.end_date)
            
            if not files:
                raise RuntimeError(
                    f"No files found for {self.DEFAULT_SOURCE}/{self.DEFAULT_MESSAGE_TYPE} "
                    f"between {self.start_date} and {self.end_date}"
                )
            
            logger.info(f"Found {len(files)} files to download")
            
            # Download and process
            results = fetcher.download_files(files)
            successful = [r for r in results if r.success]
            
            logger.info(f"Downloaded {len(successful)}/{len(files)} files")
            
            # Parse and combine
            dfs = []
            for result in successful:
                if result.local_path and result.local_path.exists():
                    df = fetcher._parse_files([result.local_path])
                    if df is not None and not df.empty:
                        dfs.append(df)
            
            if not dfs:
                raise RuntimeError("No valid data parsed from downloaded files")
            
            return pd.concat(dfs, ignore_index=True)
            
        except Exception as e:
            logger.error(f"S3 download failed: {e}")
            raise RuntimeError(
                f"Failed to download from S3: {e}. "
                "Ensure AWS credentials are configured in ~/.aws/credentials "
                "or environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. "
                "Alternatively, use use_synthetic=True for testing."
            ) from e
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the data source."""
        cache_path = self.get_cache_path()
        
        stats = {
            "use_synthetic": self.use_synthetic,
            "target_rows": self.num_rows,
            "cache_dir": str(self.cache_dir),
            "cache_path": str(cache_path),
            "is_cached": cache_path.exists(),
            "date_range": f"{self.start_date} to {self.end_date}",
            "center_location": f"({self.center_lat}, {self.center_lon})",
            "max_distance_meters": self.max_distance_meters,
        }
        
        if cache_path.exists():
            stats["cache_size_mb"] = cache_path.stat().st_size / (1024 * 1024)
            
            # Get row count from parquet metadata
            try:
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(cache_path)
                stats["actual_rows"] = parquet_file.metadata.num_rows
            except Exception:
                pass
        
        return stats


def create_wyoming_100m_cache(
    num_rows: int = 100_000_000,
    use_synthetic: bool = True,
    cache_dir: Optional[str] = None,
) -> Path:
    """
    Helper function to create and populate the Wyoming 100M cache.
    
    Args:
        num_rows: Number of rows to generate/download
        use_synthetic: Use synthetic data (True) or S3 download (False)
        cache_dir: Custom cache directory
    
    Returns:
        Path to the cached parquet file
    """
    source = Wyoming100MDataSource(
        use_synthetic=use_synthetic,
        num_rows=num_rows,
        cache_dir=Path(cache_dir) if cache_dir else None,
    )
    
    source.get_data()
    return source.get_cache_path()
