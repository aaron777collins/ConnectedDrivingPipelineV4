"""
Generate sample BSM datasets for testing.

This script creates realistic BSM (Basic Safety Message) data files
with varying sizes for integration testing, performance benchmarking,
and validation.

Usage:
    python -m Test.Data.generate_sample_datasets

Outputs:
    - Test/Data/sample_1k.csv: 1,000 rows
    - Test/Data/sample_10k.csv: 10,000 rows
    - Test/Data/sample_100k.csv: 100,000 rows
"""

import os
import sys
import csv
from datetime import datetime, timedelta
import random
import math

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from Schemas.BSMRawSchema import get_bsm_raw_column_names, TIMESTAMP_FORMAT


def generate_bsm_data(num_rows, seed=42):
    """
    Generate realistic BSM data.

    Args:
        num_rows: Number of rows to generate
        seed: Random seed for reproducibility

    Returns:
        List of dictionaries, one per row
    """
    random.seed(seed)

    # Base parameters
    base_time = datetime(2021, 4, 6, 10, 0, 0)
    base_lat = 41.25  # Wyoming coordinates
    base_lon = -105.93

    # Number of unique vehicle streams
    num_streams = min(max(10, num_rows // 100), 200)

    rows = []

    for i in range(num_rows):
        # Temporal progression (1 second intervals)
        timestamp = base_time + timedelta(seconds=i)
        timestamp_str = timestamp.strftime(TIMESTAMP_FORMAT)

        # Vehicle stream assignment (each vehicle has multiple messages)
        stream_id = i % num_streams

        # Spatial distribution with slight movement
        # Each vehicle moves slightly over time
        vehicle_offset = stream_id * 0.001
        time_offset = (i // num_streams) * 0.0001

        lat = base_lat + vehicle_offset + time_offset + random.gauss(0, 0.00001)
        lon = base_lon + vehicle_offset + time_offset + random.gauss(0, 0.00001)

        # Speed variation (0-45 m/s, with preference for 10-30 m/s)
        speed = max(0.0, min(45.0, random.gauss(20.0, 8.0)))

        # Heading (0-359 degrees, with some continuity per vehicle)
        base_heading = (stream_id * 37) % 360  # Each vehicle has a base direction
        heading = (base_heading + random.randint(-30, 30)) % 360

        # Elevation (1000-2000m, typical for Wyoming)
        elevation = 1500.0 + random.gauss(0, 100)

        # Acceleration (mostly small values, occasionally larger)
        accel_yaw = random.gauss(0, 5.0)

        # Position accuracy (typical GPS accuracy 1-10m)
        semi_major = random.uniform(1.0, 10.0)
        semi_minor = random.uniform(1.0, 10.0)

        # Create row
        row = {
            'metadata_generatedAt': timestamp_str,
            'metadata_recordType': 'bsmTx',
            'metadata_serialId_streamId': f'{stream_id:08x}',
            'metadata_serialId_bundleSize': 1,
            'metadata_serialId_bundleId': i,
            'metadata_serialId_recordId': 0,
            'metadata_serialId_serialNumber': i,
            'metadata_receivedAt': timestamp_str,
            'coreData_id': f'{stream_id:08x}',
            'coreData_secMark': (i % 60000),  # Milliseconds in minute
            'coreData_position_lat': lat,
            'coreData_position_long': lon,
            'coreData_accuracy_semiMajor': semi_major,
            'coreData_accuracy_semiMinor': semi_minor,
            'coreData_elevation': elevation,
            'coreData_accelset_accelYaw': accel_yaw,
            'coreData_speed': speed,
            'coreData_heading': heading,
            'coreData_position': f'POINT ({lon} {lat})'
        }

        rows.append(row)

    return rows


def write_csv(data, filepath):
    """
    Write data to CSV file.

    Args:
        data: List of dictionaries
        filepath: Output CSV path
    """
    if not data:
        raise ValueError("No data to write")

    columns = get_bsm_raw_column_names()

    # Create directory if needed
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"✓ Created {filepath} ({len(data):,} rows, {file_size_mb:.2f} MB)")


def main():
    """Generate all sample datasets."""
    print("Generating sample BSM datasets...")
    print()

    # Get output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Dataset sizes
    datasets = [
        ('sample_1k.csv', 1000),
        ('sample_10k.csv', 10000),
        ('sample_100k.csv', 100000)
    ]

    for filename, num_rows in datasets:
        filepath = os.path.join(script_dir, filename)
        print(f"Generating {filename} ({num_rows:,} rows)...")

        data = generate_bsm_data(num_rows, seed=42)
        write_csv(data, filepath)

    print()
    print("✓ All sample datasets generated successfully!")
    print()
    print("Files created in: Test/Data/")
    print("  - sample_1k.csv: Quick integration tests")
    print("  - sample_10k.csv: Performance benchmarks")
    print("  - sample_100k.csv: Scalability tests")


if __name__ == '__main__':
    main()
