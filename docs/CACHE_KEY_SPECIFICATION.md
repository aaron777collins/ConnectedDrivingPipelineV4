# Cache Key Specification
## Preventing Data Confusion in Multi-Source CV Pilot Data

**Version:** 1.1  
**Date:** 2026-01-28  
**Status:** Reviewed & Audited  
**Related:** [WYDOT Data Infrastructure Plan](./WYDOT_DATA_INFRASTRUCTURE_PLAN.md)  

---

## The Problem

When caching data from multiple sources (wydot, nycdot, thea), we MUST ensure:

1. **Source Isolation**: WYDOT data never gets mixed with NYCDOT data
2. **Date Range Handling**: Partial overlaps are handled correctly
3. **Message Type Isolation**: BSM never gets confused with TIM or EVENT
4. **Integrity Verification**: Corrupted/incomplete cache is detected
5. **Schema Evolution**: Old cache is invalidated when schema changes

---

## Cache Key Design

### Primary Key Structure

```
{source}/{message_type}/{year}/{month:02d}/{day:02d}
```

**Examples:**
```
wydot/BSM/2021/04/01      → wydot BSM data for April 1, 2021
nycdot/EVENT/2021/04/01   → nycdot EVENT data for April 1, 2021
thea/SPAT/2021/04/01      → thea SPAT data for April 1, 2021
```

**Critical Design Decision:** Source is the FIRST component, making it impossible to accidentally mix sources.

### File System Layout

```
cache/
├── manifest.json                      # Master index
├── wydot/
│   ├── BSM/
│   │   └── 2021/
│   │       └── 04/
│   │           ├── 01.parquet        # April 1
│   │           ├── 02.parquet        # April 2
│   │           └── ...
│   └── TIM/
│       └── 2021/
│           └── 04/
│               └── 01.parquet
├── thea/
│   ├── BSM/
│   └── SPAT/
└── nycdot/
    └── EVENT/
```

---

## Manifest Schema

The manifest (`manifest.json`) tracks all cached data and its integrity:

```json
{
  "version": 2,
  "created_at": "2026-01-28T23:00:00Z",
  "last_modified": "2026-01-28T23:30:00Z",
  
  "entries": {
    "wydot/BSM/2021/04/01": {
      "status": "complete",
      "file_path": "wydot/BSM/2021/04/01.parquet",
      "row_count": 125432,
      "file_size_bytes": 45123456,
      "source_file_count": 24,
      "checksum_sha256": "a1b2c3d4e5f6...",
      "fetched_at": "2026-01-28T22:00:00Z",
      "schema_version": "6",
      "s3_etags": ["etag1", "etag2", "..."],
      "config_hash": "cfg_abc123"
    },
    
    "nycdot/EVENT/2021/04/01": {
      "status": "complete",
      "file_path": "nycdot/EVENT/2021/04/01.parquet",
      "row_count": 8923,
      "file_size_bytes": 2345678,
      "source_file_count": 8,
      "checksum_sha256": "x1y2z3...",
      "fetched_at": "2026-01-28T22:15:00Z",
      "schema_version": "1",
      "s3_etags": ["etag1", "..."],
      "config_hash": "cfg_abc123"
    }
  },
  
  "in_progress": {
    "wydot/BSM/2021/04/03": {
      "status": "downloading",
      "started_at": "2026-01-28T23:00:00Z",
      "files_completed": 12,
      "files_total": 24,
      "bytes_downloaded": 23456789
    }
  }
}
```

### Entry Status Values

| Status | Meaning |
|--------|---------|
| `complete` | Fully downloaded and verified |
| `downloading` | Currently being fetched |
| `failed` | Download failed (will retry) |
| `corrupted` | Checksum mismatch (will re-download) |
| `expired` | TTL exceeded (will re-download) |

---

## Cache Lookup Algorithm

```python
class CacheManager:
    def get_data(
        self,
        source: str,          # "wydot", "nycdot", "thea"
        message_type: str,    # "BSM", "TIM", "SPAT", "EVENT"
        start_date: date,
        end_date: date
    ) -> DataFrame:
        """
        Retrieve data for the specified source, type, and date range.
        
        CRITICAL: source and message_type are ALWAYS explicit.
        We NEVER infer them from context or defaults.
        """
        
        # 1. VALIDATE INPUTS (fail fast on invalid source)
        if source not in VALID_SOURCES:
            raise ValueError(f"Invalid source: {source}. Valid: {VALID_SOURCES}")
        if message_type not in VALID_MESSAGE_TYPES[source]:
            raise ValueError(f"Invalid message_type {message_type} for source {source}")
        
        # 2. Build cache keys for each date in range
        dates_needed = list(date_range(start_date, end_date))
        cache_keys = [
            f"{source}/{message_type}/{d.year}/{d.month:02d}/{d.day:02d}"
            for d in dates_needed
        ]
        
        # 3. Check manifest for each key
        cached_keys = []
        missing_keys = []
        
        for key, date in zip(cache_keys, dates_needed):
            entry = self.manifest.get(key)
            
            if entry is None:
                missing_keys.append((key, date))
            elif entry.status != "complete":
                missing_keys.append((key, date))
            elif not self._verify_integrity(entry):
                self._mark_corrupted(key)
                missing_keys.append((key, date))
            elif self._is_expired(entry):
                missing_keys.append((key, date))
            else:
                cached_keys.append(key)
        
        # 4. Fetch missing data (source and type are EXPLICIT)
        if missing_keys:
            self._fetch_from_s3(
                source=source,           # Explicit!
                message_type=message_type, # Explicit!
                dates=[d for _, d in missing_keys]
            )
            cached_keys.extend([k for k, _ in missing_keys])
        
        # 5. Load and combine parquet files
        dfs = []
        for key in cached_keys:
            path = self.cache_dir / f"{key}.parquet"
            dfs.append(pd.read_parquet(path))
        
        return pd.concat(dfs, ignore_index=True)
    
    def _verify_integrity(self, entry: CacheEntry) -> bool:
        """Verify file exists and checksum matches."""
        path = self.cache_dir / entry.file_path
        if not path.exists():
            return False
        
        actual_checksum = compute_sha256(path)
        return actual_checksum == entry.checksum_sha256
```

---

## Edge Cases & Safeguards

### Case 1: Source Confusion Prevention

**Scenario:** User fetches WYDOT data, then later fetches NYCDOT data. Could they get mixed?

**Answer: NO.** The source is the first path component.

```python
# These produce COMPLETELY DIFFERENT cache keys:
get_data(source="wydot", message_type="BSM", ...)
# Cache key: wydot/BSM/2021/04/01

get_data(source="nycdot", message_type="EVENT", ...)  
# Cache key: nycdot/EVENT/2021/04/01

# They are in DIFFERENT directories. Impossible to mix.
```

### Case 2: Partial Date Range Overlap

**Scenario:** 
1. User caches April 1-10
2. User queries April 5-15

**Handling:**
```python
# Query: April 5-15
cache_keys = [
    "wydot/BSM/2021/04/05",  # ✅ Cached (from first query)
    "wydot/BSM/2021/04/06",  # ✅ Cached
    ...
    "wydot/BSM/2021/04/10",  # ✅ Cached
    "wydot/BSM/2021/04/11",  # ❌ Missing - will fetch
    ...
    "wydot/BSM/2021/04/15",  # ❌ Missing - will fetch
]
# Only April 11-15 is fetched. April 5-10 comes from cache.
```

### Case 3: Message Type Confusion

**Scenario:** User fetches BSM, then fetches TIM. Could they get mixed?

**Answer: NO.** Message type is the second path component.

```python
get_data(source="wydot", message_type="BSM", ...)
# Cache key: wydot/BSM/2021/04/01

get_data(source="wydot", message_type="TIM", ...)
# Cache key: wydot/TIM/2021/04/01

# Different directories. Impossible to mix.
```

### Case 4: Incomplete Download

**Scenario:** Download was interrupted at 50%.

**Handling:**
```python
# Before download completes, manifest shows:
{
  "wydot/BSM/2021/04/01": {
    "status": "downloading",  # NOT "complete"
    "files_completed": 12,
    "files_total": 24
  }
}

# On next query, we see status != "complete", so we re-fetch.
# The incomplete parquet file is overwritten.
```

### Case 5: Corrupted Cache File

**Scenario:** Parquet file is corrupted (disk error, etc.)

**Handling:**
```python
def _verify_integrity(self, entry):
    path = self.cache_dir / entry.file_path
    
    # Check 1: File exists
    if not path.exists():
        return False
    
    # Check 2: File size matches
    if path.stat().st_size != entry.file_size_bytes:
        return False
    
    # Check 3: Checksum matches (optional, slower)
    if self.verify_checksums:
        actual = compute_sha256(path)
        if actual != entry.checksum_sha256:
            return False
    
    return True
```

### Case 6: Schema Version Change

**Scenario:** S3 data schema changes from version 5 to version 6. Old cache is incompatible.

**Handling:**
```python
CURRENT_SCHEMA_VERSION = "6"

def _is_compatible(self, entry):
    return entry.schema_version == CURRENT_SCHEMA_VERSION

# On query, if schema version mismatches, treat as cache miss.
# Re-fetch and re-process with new schema.
```

### Case 7: Configuration Change

**Scenario:** User changes processing config (e.g., adds coordinate filtering). Old cache used different config.

**Handling:**
```python
def _compute_config_hash(self, config: DataSourceConfig) -> str:
    """Hash the config settings that affect cached data."""
    relevant = {
        "validate_schema": config.validate_schema,
        "drop_invalid": config.drop_invalid,
        "coordinate_bounds": config.coordinate_bounds,
        # ... other relevant settings
    }
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()[:12]

# Cache entries include config_hash.
# On query, if config_hash doesn't match, re-process.
```

---

## Validation Rules

### Source Validation

```python
VALID_SOURCES = {"wydot", "wydot_backup", "thea", "nycdot"}

VALID_MESSAGE_TYPES = {
    "wydot": {"BSM", "TIM"},
    "wydot_backup": {"BSM", "TIM"},
    "thea": {"BSM", "TIM", "SPAT"},
    "nycdot": {"EVENT"},
}

def validate_source_and_type(source: str, message_type: str):
    if source not in VALID_SOURCES:
        raise ValueError(f"Invalid source '{source}'. Valid: {VALID_SOURCES}")
    
    valid_types = VALID_MESSAGE_TYPES[source]
    if message_type not in valid_types:
        raise ValueError(
            f"Invalid message_type '{message_type}' for source '{source}'. "
            f"Valid types for {source}: {valid_types}"
        )
```

### Never Infer, Always Explicit

```python
# ❌ BAD: Inferring source from somewhere
def get_data(start_date, end_date):
    source = self.default_source  # DANGEROUS!
    ...

# ✅ GOOD: Always require explicit source
def get_data(source: str, message_type: str, start_date, end_date):
    validate_source_and_type(source, message_type)  # Fail fast
    ...
```

---

## Testing Scenarios

### Test 1: Source Isolation

```python
def test_source_isolation():
    cache = CacheManager("test_cache")
    
    # Fetch WYDOT data
    wydot_df = cache.get_data(
        source="wydot",
        message_type="BSM",
        start_date=date(2021, 4, 1),
        end_date=date(2021, 4, 1)
    )
    
    # Fetch NYCDOT data
    nycdot_df = cache.get_data(
        source="nycdot",
        message_type="EVENT",
        start_date=date(2021, 4, 1),
        end_date=date(2021, 4, 1)
    )
    
    # Verify completely different data
    assert "coreData_position_lat" in wydot_df.columns  # BSM field
    assert "eventHeader" in nycdot_df.columns  # EVENT field
    
    # Verify separate cache files
    assert (cache.cache_dir / "wydot/BSM/2021/04/01.parquet").exists()
    assert (cache.cache_dir / "nycdot/EVENT/2021/04/01.parquet").exists()
```

### Test 2: Message Type Isolation

```python
def test_message_type_isolation():
    cache = CacheManager("test_cache")
    
    # Fetch BSM
    bsm_df = cache.get_data("wydot", "BSM", date(2021, 4, 1), date(2021, 4, 1))
    
    # Fetch TIM
    tim_df = cache.get_data("wydot", "TIM", date(2021, 4, 1), date(2021, 4, 1))
    
    # Verify different structures
    assert "coreData" in str(bsm_df.columns)
    assert "travelerInformation" in str(tim_df.columns)  # TIM-specific
```

### Test 3: Invalid Source Rejection

```python
def test_invalid_source_rejected():
    cache = CacheManager("test_cache")
    
    with pytest.raises(ValueError, match="Invalid source"):
        cache.get_data(
            source="invalid_source",
            message_type="BSM",
            start_date=date(2021, 4, 1),
            end_date=date(2021, 4, 1)
        )

def test_invalid_message_type_for_source():
    cache = CacheManager("test_cache")
    
    # NYCDOT only has EVENT, not BSM
    with pytest.raises(ValueError, match="Invalid message_type"):
        cache.get_data(
            source="nycdot",
            message_type="BSM",  # Invalid for nycdot!
            start_date=date(2021, 4, 1),
            end_date=date(2021, 4, 1)
        )
```

---

## Summary

| Concern | Solution |
|---------|----------|
| Source confusion | Source is FIRST path component |
| Message type confusion | Message type is SECOND path component |
| Partial downloads | Status field in manifest (`downloading` vs `complete`) |
| Corrupted files | SHA256 checksum verification |
| Schema changes | Schema version in manifest |
| Config changes | Config hash in manifest |
| Invalid sources | Strict validation on every call |
| Inference errors | NEVER infer, ALWAYS require explicit params |

**Key Principle:** The cache key `{source}/{message_type}/{year}/{month}/{day}` makes it *structurally impossible* to confuse data from different sources or message types.

---

## Additional Safeguards (Added in Audit)

### Case 8: Empty S3 Prefix (No Data for Date)

**Scenario:** April 15, 2021 has no BSM data in S3 (system was down).

**Problem:** Without tracking, we'd re-fetch every time.

**Solution:**
```python
# Manifest tracks "no data" explicitly:
{
  "wydot/BSM/2021/04/15": {
    "status": "no_data",  # Not "complete", not missing
    "checked_at": "2026-01-28T22:00:00Z",
    "s3_files_found": 0
  }
}

# On cache check:
if entry.status == "no_data":
    # Return empty DataFrame, don't re-fetch
    return pd.DataFrame()
```

### Case 9: Concurrent Access

**Scenario:** Two processes try to fetch the same date simultaneously.

**Solution:** File locking per cache key.
```python
from filelock import FileLock

def save_processed(self, key, df):
    lock = FileLock(f".locks/{key.replace('/', '_')}.lock", timeout=60)
    with lock:
        # Only one process writes at a time
        temp = f".tmp_{uuid4()}.parquet"
        df.to_parquet(temp)
        os.rename(temp, f"{key}.parquet")  # Atomic
```

### Case 10: Disk Full During Write

**Solution:** Atomic writes prevent partial files.
```python
def save_processed(self, key, df):
    temp_path = self.cache_dir / f".tmp_{uuid.uuid4()}.parquet"
    final_path = self.cache_dir / f"{key}.parquet"
    
    try:
        df.to_parquet(temp_path)
        temp_path.rename(final_path)
    except OSError as e:
        temp_path.unlink(missing_ok=True)  # Clean up temp file
        if "No space left" in str(e):
            self.evict_lru(bytes_needed=temp_path.stat().st_size)
            raise  # Let caller retry
        raise
```

### Case 11: Time Zone Confusion

**Scenario:** User specifies April 1 in local time (America/Denver), but S3 uses UTC.

**Solution:** Always normalize to UTC before cache key generation.
```python
import pytz
from datetime import datetime

def normalize_date(dt: date, tz: str) -> date:
    """Convert local date to UTC date."""
    if tz == "UTC":
        return dt
    local_tz = pytz.timezone(tz)
    local_dt = local_tz.localize(datetime.combine(dt, datetime.min.time()))
    return local_dt.astimezone(pytz.UTC).date()

# Cache key always uses UTC date:
cache_key = f"{source}/{msg_type}/{utc_date.year}/..."
```

---

## Audit Checklist

- [x] Source isolation (FIRST path component)
- [x] Message type isolation (SECOND path component)
- [x] Partial downloads (status tracking)
- [x] Corrupted files (SHA256 checksums)
- [x] Schema changes (version in manifest)
- [x] Config changes (config hash)
- [x] Invalid sources (strict validation)
- [x] Never infer (always explicit params)
- [x] Empty data (no_data status)
- [x] Concurrent access (file locking)
- [x] Disk full (atomic writes)
- [x] Time zones (UTC normalization)
