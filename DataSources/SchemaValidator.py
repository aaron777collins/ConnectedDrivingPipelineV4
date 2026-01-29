"""
Schema validation for CV Pilot data messages.

Provides validators for BSM (Basic Safety Messages) and TIM (Traveler Information
Messages) with support for schema versioning across different time periods.

Usage:
    from DataSources.SchemaValidator import BSMSchemaValidator, ValidationReport
    
    validator = BSMSchemaValidator()
    
    # Single record validation
    is_valid, errors = validator.validate(record)
    
    # Batch validation with statistics
    report = validator.batch_validate(records)
    print(f"Valid: {report.valid_count}, Invalid: {report.invalid_count}")
    
    # Get canonical schema for Parquet
    schema = validator.get_canonical_schema()
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union
import re

import pyarrow as pa


@dataclass
class ValidationError:
    """A single validation error for a record."""
    record_index: int
    field_path: str
    error_message: str
    
    def __str__(self) -> str:
        return f"Record {self.record_index}: {self.field_path} - {self.error_message}"


@dataclass
class ValidationWarning:
    """A validation warning (record valid but with issues)."""
    record_index: int
    field_path: str
    warning_message: str
    
    def __str__(self) -> str:
        return f"Record {self.record_index}: {self.field_path} - {self.warning_message}"


@dataclass
class ValidationReport:
    """Report from batch validation containing statistics and errors."""
    valid_count: int = 0
    invalid_count: int = 0
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationWarning] = field(default_factory=list)
    
    @property
    def total_count(self) -> int:
        """Total number of records validated."""
        return self.valid_count + self.invalid_count
    
    @property
    def valid_ratio(self) -> float:
        """Ratio of valid records (0.0 to 1.0)."""
        if self.total_count == 0:
            return 0.0
        return self.valid_count / self.total_count
    
    @property
    def is_all_valid(self) -> bool:
        """True if all records are valid."""
        return self.invalid_count == 0
    
    def add_error(self, record_index: int, field_path: str, message: str) -> None:
        """Add a validation error."""
        self.errors.append(ValidationError(record_index, field_path, message))
    
    def add_warning(self, record_index: int, field_path: str, message: str) -> None:
        """Add a validation warning."""
        self.warnings.append(ValidationWarning(record_index, field_path, message))
    
    def __str__(self) -> str:
        return (
            f"ValidationReport(valid={self.valid_count}, invalid={self.invalid_count}, "
            f"errors={len(self.errors)}, warnings={len(self.warnings)})"
        )


def get_nested_value(data: Dict[str, Any], path: str) -> Tuple[bool, Any]:
    """
    Get a nested value from a dictionary using dot notation.
    
    Args:
        data: The dictionary to search
        path: Dot-separated path (e.g., "payload.data.coreData.speed")
    
    Returns:
        Tuple of (found: bool, value: Any)
        If not found, value is None
    """
    keys = path.split('.')
    current = data
    
    for key in keys:
        if not isinstance(current, dict):
            return False, None
        if key not in current:
            return False, None
        current = current[key]
    
    return True, current


def set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
    """
    Set a nested value in a dictionary using dot notation.
    
    Creates intermediate dictionaries as needed.
    
    Args:
        data: The dictionary to modify
        path: Dot-separated path
        value: Value to set
    """
    keys = path.split('.')
    current = data
    
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    current[keys[-1]] = value


class BaseSchemaValidator(ABC):
    """
    Abstract base class for message schema validators.
    
    Subclasses must implement:
    - REQUIRED_FIELDS: List of required field paths
    - SCHEMA_VERSIONS: Dict mapping date ranges to version numbers
    - get_canonical_schema(): Return PyArrow schema
    - _get_version_specific_fields(): Return version-specific required fields
    """
    
    # Subclasses must define these
    REQUIRED_FIELDS: List[str] = []
    SCHEMA_VERSIONS: Dict[Tuple[date, date], int] = {}
    
    def get_schema_version(self, record_date: date) -> int:
        """
        Determine the schema version for a given date.
        
        Args:
            record_date: The date of the record
        
        Returns:
            Schema version number
        
        Raises:
            ValueError: If date doesn't match any known schema version
        """
        for (start, end), version in self.SCHEMA_VERSIONS.items():
            if start <= record_date <= end:
                return version
        
        raise ValueError(f"No schema version found for date {record_date}")
    
    def _extract_record_date(self, record: Dict[str, Any]) -> Optional[date]:
        """
        Extract the date from a record's metadata.
        
        Returns None if date cannot be extracted.
        """
        found, timestamp = get_nested_value(record, 'metadata.recordGeneratedAt')
        if not found or not timestamp:
            return None
        
        # Handle various timestamp formats
        if isinstance(timestamp, str):
            # ISO format: 2021-04-01T12:30:45.123Z
            match = re.match(r'(\d{4})-(\d{2})-(\d{2})', timestamp)
            if match:
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        
        return None
    
    def _validate_required_fields(
        self, 
        record: Dict[str, Any], 
        record_index: int,
        errors: List[str]
    ) -> None:
        """
        Validate that all required fields are present.
        
        Args:
            record: The record to validate
            record_index: Index of record in batch (for error reporting)
            errors: List to append error messages to
        """
        for field_path in self.REQUIRED_FIELDS:
            found, value = get_nested_value(record, field_path)
            if not found:
                errors.append(f"Missing required field: {field_path}")
            elif value is None:
                errors.append(f"Required field is null: {field_path}")
    
    @abstractmethod
    def _validate_field_types(
        self,
        record: Dict[str, Any],
        record_index: int,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """
        Validate field types and value ranges.
        
        Subclasses implement type-specific validation.
        """
        pass
    
    @abstractmethod
    def get_canonical_schema(self) -> pa.Schema:
        """
        Return the canonical PyArrow schema for this message type.
        
        Used to ensure all Parquet files have consistent columns
        regardless of schema version variations.
        """
        pass
    
    def validate(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single record.
        
        Args:
            record: Dictionary representing the message record
        
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        errors: List[str] = []
        warnings: List[str] = []
        
        # Check required fields
        self._validate_required_fields(record, 0, errors)
        
        # Check field types and values
        self._validate_field_types(record, 0, errors, warnings)
        
        return len(errors) == 0, errors
    
    def batch_validate(
        self, 
        records: List[Dict[str, Any]],
        max_errors: int = 1000
    ) -> ValidationReport:
        """
        Validate a batch of records and return statistics.
        
        Args:
            records: List of record dictionaries
            max_errors: Maximum errors to collect (prevents memory issues)
        
        Returns:
            ValidationReport with counts and error details
        """
        report = ValidationReport()
        
        for idx, record in enumerate(records):
            errors: List[str] = []
            warnings: List[str] = []
            
            # Check required fields
            self._validate_required_fields(record, idx, errors)
            
            # Check field types and values (if basic validation passed)
            if not errors:
                self._validate_field_types(record, idx, errors, warnings)
            
            if errors:
                report.invalid_count += 1
                if len(report.errors) < max_errors:
                    for error in errors:
                        # Parse field from error message
                        field_path = self._extract_field_from_error(error)
                        report.add_error(idx, field_path, error)
            else:
                report.valid_count += 1
            
            # Add warnings regardless of validity
            for warning in warnings:
                if len(report.warnings) < max_errors:
                    field_path = self._extract_field_from_error(warning)
                    report.add_warning(idx, field_path, warning)
        
        return report
    
    def _extract_field_from_error(self, error: str) -> str:
        """Extract field path from error message, or return 'unknown'."""
        # Pattern: "... field: path.to.field" or "path.to.field ..."
        patterns = [
            r'field[:\s]+([a-zA-Z_.]+)',
            r'([a-zA-Z_.]+)\s+(?:is|must|should)',
        ]
        for pattern in patterns:
            match = re.search(pattern, error)
            if match:
                return match.group(1)
        return "unknown"


class BSMSchemaValidator(BaseSchemaValidator):
    """
    Validator for Basic Safety Message (BSM) records.
    
    BSM messages contain vehicle state information including:
    - Position (latitude, longitude, elevation)
    - Motion (speed, heading, acceleration)
    - Vehicle attributes (size, classification)
    
    Schema versions evolved over the CV Pilot program:
    - Version 2 (2017-01-01 to 2018-01-17): Initial format
    - Version 3 (2018-01-18 to 2020-06-30): Added fields, format changes
    - Version 6 (2020-07-01 onwards): Current format
    """
    
    REQUIRED_FIELDS = [
        'metadata.recordGeneratedAt',
        'payload.data.coreData.position.latitude',
        'payload.data.coreData.position.longitude',
        'payload.data.coreData.elevation',
        'payload.data.coreData.speed',
        'payload.data.coreData.heading',
    ]
    
    SCHEMA_VERSIONS = {
        (date(2017, 1, 1), date(2018, 1, 17)): 2,
        (date(2018, 1, 18), date(2020, 6, 30)): 3,
        (date(2020, 7, 1), date(2099, 12, 31)): 6,
    }
    
    # Validation ranges for BSM fields
    VALIDATION_RANGES = {
        'latitude': (-90.0, 90.0),
        'longitude': (-180.0, 180.0),
        'elevation': (-500.0, 10000.0),  # meters, -500 to 10km
        'speed': (0.0, 200.0),  # m/s, 0 to ~450 mph
        'heading': (0.0, 360.0),  # degrees
    }
    
    # Wyoming coordinate bounds (for warning generation)
    WYOMING_BOUNDS = {
        'lat_min': 40.0,
        'lat_max': 46.0,
        'lon_min': -112.0,
        'lon_max': -104.0,
    }
    
    def _validate_field_types(
        self,
        record: Dict[str, Any],
        record_index: int,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate BSM-specific field types and ranges."""
        
        # Validate latitude
        found, lat = get_nested_value(record, 'payload.data.coreData.position.latitude')
        if found and lat is not None:
            if not isinstance(lat, (int, float)):
                errors.append(f"latitude must be numeric, got {type(lat).__name__}")
            else:
                min_val, max_val = self.VALIDATION_RANGES['latitude']
                if not (min_val <= lat <= max_val):
                    errors.append(f"latitude {lat} out of range [{min_val}, {max_val}]")
                elif not (self.WYOMING_BOUNDS['lat_min'] <= lat <= self.WYOMING_BOUNDS['lat_max']):
                    warnings.append(f"latitude {lat} outside Wyoming bounds")
        
        # Validate longitude
        found, lon = get_nested_value(record, 'payload.data.coreData.position.longitude')
        if found and lon is not None:
            if not isinstance(lon, (int, float)):
                errors.append(f"longitude must be numeric, got {type(lon).__name__}")
            else:
                min_val, max_val = self.VALIDATION_RANGES['longitude']
                if not (min_val <= lon <= max_val):
                    errors.append(f"longitude {lon} out of range [{min_val}, {max_val}]")
                elif not (self.WYOMING_BOUNDS['lon_min'] <= lon <= self.WYOMING_BOUNDS['lon_max']):
                    warnings.append(f"longitude {lon} outside Wyoming bounds")
        
        # Validate elevation
        found, elev = get_nested_value(record, 'payload.data.coreData.elevation')
        if found and elev is not None:
            if not isinstance(elev, (int, float)):
                errors.append(f"elevation must be numeric, got {type(elev).__name__}")
            else:
                min_val, max_val = self.VALIDATION_RANGES['elevation']
                if not (min_val <= elev <= max_val):
                    errors.append(f"elevation {elev} out of range [{min_val}, {max_val}]")
        
        # Validate speed
        found, speed = get_nested_value(record, 'payload.data.coreData.speed')
        if found and speed is not None:
            if not isinstance(speed, (int, float)):
                errors.append(f"speed must be numeric, got {type(speed).__name__}")
            else:
                min_val, max_val = self.VALIDATION_RANGES['speed']
                if not (min_val <= speed <= max_val):
                    errors.append(f"speed {speed} out of range [{min_val}, {max_val}]")
        
        # Validate heading
        found, heading = get_nested_value(record, 'payload.data.coreData.heading')
        if found and heading is not None:
            if not isinstance(heading, (int, float)):
                errors.append(f"heading must be numeric, got {type(heading).__name__}")
            else:
                min_val, max_val = self.VALIDATION_RANGES['heading']
                if not (min_val <= heading <= max_val):
                    errors.append(f"heading {heading} out of range [{min_val}, {max_val}]")
        
        # Validate timestamp format
        found, timestamp = get_nested_value(record, 'metadata.recordGeneratedAt')
        if found and timestamp is not None:
            if not isinstance(timestamp, str):
                errors.append(f"recordGeneratedAt must be string, got {type(timestamp).__name__}")
            elif not re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', timestamp):
                warnings.append(f"recordGeneratedAt format unexpected: {timestamp[:30]}")
    
    def get_canonical_schema(self) -> pa.Schema:
        """
        Return the canonical PyArrow schema for BSM records.
        
        This schema represents the flattened structure used for Parquet storage,
        with consistent column names across all schema versions.
        """
        return pa.schema([
            # Metadata fields
            ('record_generated_at', pa.timestamp('us', tz='UTC')),
            ('record_generated_by', pa.string()),
            ('schema_version', pa.int32()),
            
            # Core position data
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('elevation', pa.float64()),
            
            # Core motion data
            ('speed', pa.float64()),
            ('heading', pa.float64()),
            
            # Optional acceleration (may be null)
            ('accel_long', pa.float64()),
            ('accel_lat', pa.float64()),
            ('accel_vert', pa.float64()),
            ('yaw_rate', pa.float64()),
            
            # Vehicle identification
            ('temp_id', pa.string()),
            ('msg_count', pa.int32()),
            ('sec_mark', pa.int32()),
            
            # Vehicle attributes (optional)
            ('vehicle_length', pa.float64()),
            ('vehicle_width', pa.float64()),
            
            # Accuracy metrics (optional)
            ('position_accuracy', pa.float64()),
            ('elevation_accuracy', pa.float64()),
            ('speed_accuracy', pa.float64()),
            ('heading_accuracy', pa.float64()),
            
            # Transmission and brake status (optional)
            ('transmission_state', pa.string()),
            ('brake_status', pa.string()),
            
            # Raw payload for advanced processing (optional)
            ('raw_payload_json', pa.string()),
        ])
    
    def flatten_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a nested BSM record into a flat dictionary matching canonical schema.
        
        Args:
            record: Nested BSM record dictionary
        
        Returns:
            Flat dictionary with keys matching canonical schema columns
        """
        flat: Dict[str, Any] = {}
        
        # Metadata
        found, val = get_nested_value(record, 'metadata.recordGeneratedAt')
        flat['record_generated_at'] = val if found else None
        
        found, val = get_nested_value(record, 'metadata.recordGeneratedBy')
        flat['record_generated_by'] = val if found else None
        
        found, val = get_nested_value(record, 'metadata.schemaVersion')
        flat['schema_version'] = val if found else None
        
        # Core position
        found, val = get_nested_value(record, 'payload.data.coreData.position.latitude')
        flat['latitude'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.position.longitude')
        flat['longitude'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.elevation')
        flat['elevation'] = float(val) if found and val is not None else None
        
        # Core motion
        found, val = get_nested_value(record, 'payload.data.coreData.speed')
        flat['speed'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.heading')
        flat['heading'] = float(val) if found and val is not None else None
        
        # Acceleration (nested under accelSet)
        found, val = get_nested_value(record, 'payload.data.coreData.accelSet.accelLong')
        flat['accel_long'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.accelSet.accelLat')
        flat['accel_lat'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.accelSet.accelVert')
        flat['accel_vert'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.accelSet.accelYaw')
        flat['yaw_rate'] = float(val) if found and val is not None else None
        
        # Vehicle identification
        found, val = get_nested_value(record, 'payload.data.coreData.id')
        flat['temp_id'] = str(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.msgCnt')
        flat['msg_count'] = int(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.secMark')
        flat['sec_mark'] = int(val) if found and val is not None else None
        
        # Vehicle size
        found, val = get_nested_value(record, 'payload.data.coreData.size.length')
        flat['vehicle_length'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.size.width')
        flat['vehicle_width'] = float(val) if found and val is not None else None
        
        # Accuracy
        found, val = get_nested_value(record, 'payload.data.coreData.accuracy.semiMajor')
        flat['position_accuracy'] = float(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.accuracy.elevation')
        flat['elevation_accuracy'] = float(val) if found and val is not None else None
        
        # Transmission and brakes
        found, val = get_nested_value(record, 'payload.data.coreData.transmission')
        flat['transmission_state'] = str(val) if found and val is not None else None
        
        found, val = get_nested_value(record, 'payload.data.coreData.brakes.wheelBrakes')
        flat['brake_status'] = str(val) if found and val is not None else None
        
        # Speed and heading accuracy not always present
        flat['speed_accuracy'] = None
        flat['heading_accuracy'] = None
        
        # Raw payload (optional, for debugging)
        flat['raw_payload_json'] = None
        
        return flat


class TIMSchemaValidator(BaseSchemaValidator):
    """
    Validator for Traveler Information Message (TIM) records.
    
    TIM messages contain roadway information such as:
    - Work zone alerts
    - Speed advisories
    - Road conditions
    - Geographic regions of applicability
    
    TIM has a more complex structure than BSM with:
    - Frame structure (travelerInfoType, msgId, packetId)
    - Geographic regions (anchor points, paths)
    - Advisory information (ITISCodes, text)
    """
    
    REQUIRED_FIELDS = [
        'metadata.recordGeneratedAt',
        'payload.data.MessageFrame.value.TravelerInformation',
    ]
    
    SCHEMA_VERSIONS = {
        (date(2017, 1, 1), date(2018, 1, 17)): 2,
        (date(2018, 1, 18), date(2020, 6, 30)): 3,
        (date(2020, 7, 1), date(2099, 12, 31)): 6,
    }
    
    def _validate_field_types(
        self,
        record: Dict[str, Any],
        record_index: int,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate TIM-specific field types."""
        
        # Validate timestamp format
        found, timestamp = get_nested_value(record, 'metadata.recordGeneratedAt')
        if found and timestamp is not None:
            if not isinstance(timestamp, str):
                errors.append(f"recordGeneratedAt must be string, got {type(timestamp).__name__}")
            elif not re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', timestamp):
                warnings.append(f"recordGeneratedAt format unexpected: {timestamp[:30]}")
        
        # Validate TravelerInformation structure exists and is dict
        found, tim_data = get_nested_value(
            record, 'payload.data.MessageFrame.value.TravelerInformation'
        )
        if found and tim_data is not None:
            if not isinstance(tim_data, dict):
                errors.append(
                    f"TravelerInformation must be object, got {type(tim_data).__name__}"
                )
            else:
                # Check for dataFrames array
                data_frames = tim_data.get('dataFrames')
                if data_frames is None:
                    warnings.append("TravelerInformation.dataFrames is missing")
                elif not isinstance(data_frames, list):
                    errors.append(
                        f"dataFrames must be array, got {type(data_frames).__name__}"
                    )
    
    def get_canonical_schema(self) -> pa.Schema:
        """
        Return the canonical PyArrow schema for TIM records.
        
        TIM records have a complex nested structure. This schema represents
        a flattened view with the most commonly used fields.
        """
        return pa.schema([
            # Metadata
            ('record_generated_at', pa.timestamp('us', tz='UTC')),
            ('record_generated_by', pa.string()),
            ('schema_version', pa.int32()),
            
            # Message identification
            ('msg_id', pa.string()),
            ('packet_id', pa.string()),
            ('url_b', pa.string()),
            
            # Temporal
            ('start_year', pa.int32()),
            ('start_time', pa.int64()),  # minutes since year start
            ('duration_time', pa.int64()),  # minutes
            
            # Geographic anchor
            ('anchor_lat', pa.float64()),
            ('anchor_lon', pa.float64()),
            ('anchor_elevation', pa.float64()),
            
            # Frame information
            ('frame_type', pa.int32()),
            ('msg_cnt', pa.int32()),
            
            # Advisory content (as JSON arrays for flexibility)
            ('itis_codes', pa.string()),  # JSON array of codes
            ('advisory_text', pa.string()),  # Combined advisory text
            
            # Geographic extent
            ('direction', pa.string()),
            ('extent', pa.int32()),
            
            # Region description
            ('region_description', pa.string()),
            ('path_json', pa.string()),  # JSON representation of path
            
            # Raw payload
            ('raw_payload_json', pa.string()),
        ])
    
    def flatten_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a nested TIM record into a flat dictionary matching canonical schema.
        
        Args:
            record: Nested TIM record dictionary
        
        Returns:
            Flat dictionary with keys matching canonical schema columns
        """
        import json
        
        flat: Dict[str, Any] = {}
        
        # Metadata
        found, val = get_nested_value(record, 'metadata.recordGeneratedAt')
        flat['record_generated_at'] = val if found else None
        
        found, val = get_nested_value(record, 'metadata.recordGeneratedBy')
        flat['record_generated_by'] = val if found else None
        
        found, val = get_nested_value(record, 'metadata.schemaVersion')
        flat['schema_version'] = val if found else None
        
        # Get TravelerInformation
        tim_path = 'payload.data.MessageFrame.value.TravelerInformation'
        found, tim = get_nested_value(record, tim_path)
        
        if not found or tim is None:
            # Fill with nulls
            for key in ['msg_id', 'packet_id', 'url_b', 'start_year', 'start_time',
                       'duration_time', 'anchor_lat', 'anchor_lon', 'anchor_elevation',
                       'frame_type', 'msg_cnt', 'itis_codes', 'advisory_text',
                       'direction', 'extent', 'region_description', 'path_json']:
                flat[key] = None
            flat['raw_payload_json'] = None
            return flat
        
        # Message ID fields
        flat['msg_id'] = tim.get('msgCnt')
        flat['packet_id'] = tim.get('packetID')
        flat['url_b'] = tim.get('urlB')
        
        # Get first dataFrame for common fields
        data_frames = tim.get('dataFrames', [])
        if data_frames and isinstance(data_frames, list) and len(data_frames) > 0:
            frame = data_frames[0]
            
            flat['frame_type'] = frame.get('frameType')
            flat['msg_cnt'] = frame.get('msgId', {}).get('roadSignID', {}).get('mutcdCode')
            flat['start_year'] = frame.get('startYear')
            flat['start_time'] = frame.get('startTime')
            flat['duration_time'] = frame.get('duratonTime')  # Note: typo in actual data
            
            # Geographic anchor from region
            regions = frame.get('regions', [])
            if regions and len(regions) > 0:
                region = regions[0]
                anchor = region.get('anchor', {})
                flat['anchor_lat'] = anchor.get('lat')
                flat['anchor_lon'] = anchor.get('long')
                flat['anchor_elevation'] = anchor.get('elevation')
                flat['direction'] = region.get('direction')
                flat['region_description'] = str(region.get('description', ''))
                
                # Path as JSON
                path = region.get('path')
                flat['path_json'] = json.dumps(path) if path else None
            else:
                flat['anchor_lat'] = None
                flat['anchor_lon'] = None
                flat['anchor_elevation'] = None
                flat['direction'] = None
                flat['region_description'] = None
                flat['path_json'] = None
            
            flat['extent'] = frame.get('extent')
            
            # ITIS codes and advisory content
            content = frame.get('content', {})
            advisory = content.get('advisory', {})
            itis_list = advisory.get('SEQUENCE', [])
            
            # Extract ITIS codes
            codes = []
            texts = []
            for item in itis_list:
                if isinstance(item, dict):
                    if 'itis' in item:
                        codes.append(item['itis'])
                    if 'text' in item:
                        texts.append(item['text'])
            
            flat['itis_codes'] = json.dumps(codes) if codes else None
            flat['advisory_text'] = ' '.join(texts) if texts else None
        else:
            # No frames
            for key in ['frame_type', 'msg_cnt', 'start_year', 'start_time',
                       'duration_time', 'anchor_lat', 'anchor_lon', 'anchor_elevation',
                       'direction', 'extent', 'region_description', 'path_json',
                       'itis_codes', 'advisory_text']:
                flat[key] = None
        
        # Raw payload (optional)
        flat['raw_payload_json'] = None
        
        return flat


def get_validator(message_type: str) -> BaseSchemaValidator:
    """
    Get the appropriate validator for a message type.
    
    Args:
        message_type: 'BSM', 'TIM', etc.
    
    Returns:
        Appropriate validator instance
    
    Raises:
        ValueError: If message type is not supported
    """
    validators = {
        'BSM': BSMSchemaValidator,
        'TIM': TIMSchemaValidator,
    }
    
    msg_type_upper = message_type.upper()
    if msg_type_upper not in validators:
        raise ValueError(
            f"No validator for message type '{message_type}'. "
            f"Supported: {list(validators.keys())}"
        )
    
    return validators[msg_type_upper]()


# Export public API
__all__ = [
    'BaseSchemaValidator',
    'BSMSchemaValidator',
    'TIMSchemaValidator',
    'ValidationReport',
    'ValidationError',
    'ValidationWarning',
    'get_validator',
    'get_nested_value',
    'set_nested_value',
]
