"""
Tests for DataSources config validation.

These tests verify that Pydantic validation correctly:
- Rejects invalid sources
- Rejects invalid message types for sources
- Validates date ranges
- Generates correct cache keys
"""

import pytest
from datetime import date
import sys
sys.path.insert(0, '/home/ubuntu/ConnectedDrivingPipelineV4')

from DataSources.config import (
    DataSourceConfig,
    DateRangeConfig,
    validate_source_and_type,
    generate_cache_key,
    parse_cache_key,
    normalize_date_to_utc,
    VALID_SOURCES,
    VALID_MESSAGE_TYPES,
)


class TestSourceValidation:
    """Test that invalid sources are rejected."""
    
    def test_valid_sources_accepted(self):
        """All valid sources should be accepted."""
        for source in VALID_SOURCES:
            msg_type = list(VALID_MESSAGE_TYPES[source])[0]
            config = DataSourceConfig(
                source=source,
                message_type=msg_type,
                date_range={"start_date": "2021-04-01"}
            )
            assert config.source == source
    
    def test_invalid_source_rejected(self):
        """Invalid source should raise ValueError."""
        with pytest.raises(ValueError, match="Input should be"):
            DataSourceConfig(
                source="invalid_source",
                message_type="BSM",
                date_range={"start_date": "2021-04-01"}
            )
    
    def test_validate_source_and_type_function(self):
        """validate_source_and_type should reject invalid combinations."""
        # Valid
        validate_source_and_type("wydot", "BSM")
        validate_source_and_type("nycdot", "EVENT")
        
        # Invalid source
        with pytest.raises(ValueError, match="Invalid source"):
            validate_source_and_type("invalid", "BSM")
        
        # Invalid type for source
        with pytest.raises(ValueError, match="Invalid message_type"):
            validate_source_and_type("nycdot", "BSM")


class TestMessageTypeValidation:
    """Test message type validation per source."""
    
    def test_wydot_valid_types(self):
        """WYDOT should accept BSM and TIM only."""
        for msg_type in ["BSM", "TIM"]:
            config = DataSourceConfig(
                source="wydot",
                message_type=msg_type,
                date_range={"start_date": "2021-04-01"}
            )
            assert config.message_type == msg_type
    
    def test_wydot_invalid_type(self):
        """WYDOT should reject SPAT and EVENT."""
        with pytest.raises(ValueError, match="Invalid message_type"):
            DataSourceConfig(
                source="wydot",
                message_type="SPAT",
                date_range={"start_date": "2021-04-01"}
            )
        
        with pytest.raises(ValueError, match="Invalid message_type"):
            DataSourceConfig(
                source="wydot",
                message_type="EVENT",
                date_range={"start_date": "2021-04-01"}
            )
    
    def test_nycdot_only_event(self):
        """NYCDOT should only accept EVENT."""
        config = DataSourceConfig(
            source="nycdot",
            message_type="EVENT",
            date_range={"start_date": "2021-04-01"}
        )
        assert config.message_type == "EVENT"
        
        with pytest.raises(ValueError, match="Invalid message_type"):
            DataSourceConfig(
                source="nycdot",
                message_type="BSM",
                date_range={"start_date": "2021-04-01"}
            )
    
    def test_thea_accepts_spat(self):
        """THEA should accept SPAT (unlike WYDOT)."""
        config = DataSourceConfig(
            source="thea",
            message_type="SPAT",
            date_range={"start_date": "2021-04-01"}
        )
        assert config.message_type == "SPAT"


class TestDateRangeValidation:
    """Test date range validation."""
    
    def test_end_before_start_rejected(self):
        """End date before start date should be rejected."""
        with pytest.raises(ValueError, match="end_date must be >= start_date"):
            DateRangeConfig(
                start_date=date(2021, 4, 30),
                end_date=date(2021, 4, 1)
            )
    
    def test_days_back_must_be_positive(self):
        """days_back must be positive."""
        with pytest.raises(ValueError, match="days_back must be positive"):
            DateRangeConfig(days_back=0)
        
        with pytest.raises(ValueError, match="days_back must be positive"):
            DateRangeConfig(days_back=-5)
    
    def test_must_have_dates_or_days_back(self):
        """Must specify either dates or days_back."""
        with pytest.raises(ValueError, match="Must specify either"):
            DateRangeConfig()
    
    def test_valid_timezone(self):
        """Valid timezone should be accepted."""
        config = DateRangeConfig(
            start_date=date(2021, 4, 1),
            timezone="America/Denver"
        )
        assert config.timezone == "America/Denver"
    
    def test_invalid_timezone_rejected(self):
        """Invalid timezone should be rejected."""
        with pytest.raises(ValueError, match="Unknown timezone"):
            DateRangeConfig(
                start_date=date(2021, 4, 1),
                timezone="Invalid/Zone"
            )


class TestCacheKeys:
    """Test cache key generation and parsing."""
    
    def test_generate_cache_key(self):
        """Cache key should have correct format."""
        key = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
        assert key == "wydot/BSM/2021/04/01"
        
        key = generate_cache_key("nycdot", "EVENT", date(2021, 12, 25))
        assert key == "nycdot/EVENT/2021/12/25"
    
    def test_parse_cache_key(self):
        """Should correctly parse cache key back to components."""
        source, msg_type, dt = parse_cache_key("wydot/BSM/2021/04/01")
        assert source == "wydot"
        assert msg_type == "BSM"
        assert dt == date(2021, 4, 1)
    
    def test_source_first_in_key(self):
        """Source must be FIRST component for isolation."""
        key = generate_cache_key("wydot", "BSM", date(2021, 4, 1))
        assert key.startswith("wydot/")
        
        key = generate_cache_key("nycdot", "EVENT", date(2021, 4, 1))
        assert key.startswith("nycdot/")


class TestTimezoneNormalization:
    """Test UTC normalization for dates."""
    
    def test_utc_unchanged(self):
        """UTC dates should not change."""
        dt = date(2021, 4, 1)
        result = normalize_date_to_utc(dt, "UTC")
        assert result == dt
    
    def test_denver_to_utc(self):
        """Denver time should be converted correctly."""
        # April 1 in Denver (MDT, UTC-6) could map to different UTC date
        # depending on the hour. Since we use start of day, it stays same date.
        dt = date(2021, 4, 1)
        result = normalize_date_to_utc(dt, "America/Denver")
        # Start of day in Denver (midnight MDT) is 6am UTC same day
        assert result == date(2021, 4, 1)


class TestConfigHash:
    """Test config hash for cache invalidation."""
    
    def test_same_config_same_hash(self):
        """Same config should produce same hash."""
        config1 = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01"}
        )
        config2 = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01"}
        )
        assert config1.compute_config_hash() == config2.compute_config_hash()
    
    def test_different_validation_different_hash(self):
        """Different validation settings should produce different hash."""
        config1 = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01"},
            validation={"enabled": True}
        )
        config2 = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            date_range={"start_date": "2021-04-01"},
            validation={"enabled": False}
        )
        assert config1.compute_config_hash() != config2.compute_config_hash()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
