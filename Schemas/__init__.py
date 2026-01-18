"""
PySpark schema definitions for BSM (Basic Safety Message) data.

This package contains schema definitions for raw and processed BSM data,
used for data validation and type-safe PySpark DataFrame operations.
"""

from .BSMRawSchema import get_bsm_raw_schema
from .BSMProcessedSchema import get_bsm_processed_schema

__all__ = [
    'get_bsm_raw_schema',
    'get_bsm_processed_schema',
]
