"""
Configuration models for DataSources module.

Uses Pydantic for validation with fail-fast behavior on invalid configs.
"""

from datetime import date, datetime
from pathlib import Path
from typing import Optional, Literal, Set, Dict, Any
import hashlib
import json

from pydantic import BaseModel, field_validator, model_validator, Field
import pytz


# Valid sources and their message types
VALID_SOURCES: Set[str] = {"wydot", "wydot_backup", "thea", "nycdot"}

VALID_MESSAGE_TYPES: Dict[str, Set[str]] = {
    "wydot": {"BSM", "TIM"},
    "wydot_backup": {"BSM", "TIM"},
    "thea": {"BSM", "TIM", "SPAT"},
    "nycdot": {"EVENT"},
}


def normalize_date_to_utc(dt: date, timezone: str) -> date:
    """
    Convert a date from local timezone to UTC.
    
    S3 data is stored in UTC, so all queries must use UTC dates.
    """
    if timezone == "UTC":
        return dt
    
    local_tz = pytz.timezone(timezone)
    # Treat date as start of day in local timezone
    local_dt = local_tz.localize(datetime.combine(dt, datetime.min.time()))
    utc_dt = local_dt.astimezone(pytz.UTC)
    return utc_dt.date()


class DateRangeConfig(BaseModel):
    """Date range configuration with validation."""
    
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    days_back: Optional[int] = None  # Relative to today
    timezone: str = "UTC"  # Input timezone
    
    @field_validator('timezone')
    @classmethod
    def valid_timezone(cls, v: str) -> str:
        try:
            pytz.timezone(v)
        except pytz.UnknownTimeZoneError:
            raise ValueError(f"Unknown timezone: {v}")
        return v
    
    @field_validator('days_back')
    @classmethod
    def days_back_positive(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError('days_back must be positive')
        return v
    
    @model_validator(mode='after')
    def validate_dates(self) -> 'DateRangeConfig':
        if self.end_date and self.start_date and self.end_date < self.start_date:
            raise ValueError('end_date must be >= start_date')
        
        # Must have either absolute dates or days_back
        if self.start_date is None and self.days_back is None:
            raise ValueError('Must specify either start_date or days_back')
        
        return self
    
    def get_effective_dates(self) -> tuple[date, date]:
        """
        Get the effective start and end dates, normalized to UTC.
        
        Resolves days_back to absolute dates.
        """
        if self.days_back is not None:
            end = date.today()
            start = date.today() - __import__('datetime').timedelta(days=self.days_back)
        else:
            start = self.start_date
            end = self.end_date or self.start_date
        
        # Normalize to UTC
        start_utc = normalize_date_to_utc(start, self.timezone)
        end_utc = normalize_date_to_utc(end, self.timezone)
        
        return start_utc, end_utc


class CacheConfig(BaseModel):
    """Cache configuration."""
    
    directory: Path = Path("data/cache")
    ttl_days: Optional[int] = Field(default=None)  # None = never expire (for historical data)
    max_size_gb: float = Field(default=50.0, ge=1.0)
    verify_checksums: bool = True


class DownloadConfig(BaseModel):
    """Download configuration."""
    
    max_workers: int = Field(default=4, ge=1, le=16)
    retry_attempts: int = Field(default=3, ge=1)
    retry_delay: float = Field(default=1.0, ge=0.1)
    timeout_seconds: int = Field(default=300, ge=30)
    rate_limit_per_second: float = Field(default=10.0, ge=1.0)
    
    # AWS authentication - required since bucket policy change
    aws_profile: Optional[str] = None  # Use specific profile from ~/.aws/credentials
    use_anonymous: bool = False  # Set True to try anonymous access (may not work)


class MemoryConfig(BaseModel):
    """Memory usage configuration."""
    
    max_usage_gb: float = Field(default=24.0, ge=4.0)
    partition_size_mb: int = Field(default=128, ge=16)


class ValidationConfig(BaseModel):
    """Data validation configuration."""
    
    enabled: bool = True
    drop_invalid_records: bool = True
    coordinate_bounds: Optional[Dict[str, float]] = None


class DataSourceConfig(BaseModel):
    """
    Main configuration for data source access.
    
    Validates source/message_type combinations and all settings.
    """
    
    # Source identification - ALWAYS EXPLICIT, NEVER INFERRED
    bucket: str = "usdot-its-cvpilot-publicdata"
    source: Literal["wydot", "wydot_backup", "thea", "nycdot"]
    message_type: str
    
    # Date range
    date_range: DateRangeConfig
    
    # Sub-configs
    cache: CacheConfig = CacheConfig()
    download: DownloadConfig = DownloadConfig()
    memory: MemoryConfig = MemoryConfig()
    validation: ValidationConfig = ValidationConfig()
    
    @field_validator('message_type')
    @classmethod
    def valid_message_type(cls, v: str) -> str:
        # Basic validation - full check in model_validator
        v = v.upper()
        all_types = set()
        for types in VALID_MESSAGE_TYPES.values():
            all_types.update(types)
        if v not in all_types:
            raise ValueError(f"Unknown message_type: {v}")
        return v
    
    @model_validator(mode='after')
    def valid_message_type_for_source(self) -> 'DataSourceConfig':
        valid_types = VALID_MESSAGE_TYPES.get(self.source, set())
        if self.message_type not in valid_types:
            raise ValueError(
                f"Invalid message_type '{self.message_type}' for source '{self.source}'. "
                f"Valid types: {valid_types}"
            )
        return self
    
    def compute_config_hash(self) -> str:
        """
        Compute hash of settings that affect cached data.
        
        Used to detect config changes that invalidate cache.
        """
        relevant = {
            "source": self.source,
            "message_type": self.message_type,
            "validation_enabled": self.validation.enabled,
            "drop_invalid": self.validation.drop_invalid_records,
            "coordinate_bounds": self.validation.coordinate_bounds,
        }
        return hashlib.sha256(
            json.dumps(relevant, sort_keys=True, default=str).encode()
        ).hexdigest()[:12]
    
    @classmethod
    def from_yaml(cls, path: Path) -> 'DataSourceConfig':
        """Load and validate configuration from YAML file."""
        import yaml
        
        with open(path) as f:
            raw = yaml.safe_load(f)
        
        return cls(**raw)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourceConfig':
        """Create config from dictionary."""
        return cls(**data)


def validate_source_and_type(source: str, message_type: str) -> None:
    """
    Validate source and message type combination.
    
    Raises ValueError if invalid. Use this for quick validation without
    creating a full config object.
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Invalid source '{source}'. Valid: {VALID_SOURCES}")
    
    valid_types = VALID_MESSAGE_TYPES[source]
    if message_type.upper() not in valid_types:
        raise ValueError(
            f"Invalid message_type '{message_type}' for source '{source}'. "
            f"Valid types for {source}: {valid_types}"
        )


def generate_cache_key(source: str, message_type: str, dt: date) -> str:
    """
    Generate cache key for a specific source, type, and date.
    
    Format: {source}/{message_type}/{year}/{month:02d}/{day:02d}
    
    Source is FIRST to ensure isolation.
    """
    return f"{source}/{message_type}/{dt.year}/{dt.month:02d}/{dt.day:02d}"


def parse_cache_key(key: str) -> tuple[str, str, date]:
    """
    Parse a cache key back into components.
    
    Returns: (source, message_type, date)
    """
    parts = key.split('/')
    if len(parts) != 5:
        raise ValueError(f"Invalid cache key format: {key}")
    
    source, msg_type, year, month, day = parts
    return source, msg_type, date(int(year), int(month), int(day))
