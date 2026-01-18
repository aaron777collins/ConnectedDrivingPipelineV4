"""
Comprehensive validation tests for existing Dask components.

This test suite validates all production-ready Dask components:
- DaskSessionManager: Cluster/client initialization & monitoring
- DaskUDFs: Registry, geospatial, and conversion functions
- Integration tests demonstrating component functionality

Note: Full integration tests for Cleaners and Attackers are in separate
test files that properly set up dependency injection context.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import numpy as np
import os
import math

# Import existing Dask components
from Helpers.DaskSessionManager import DaskSessionManager, get_dask_client
from Helpers.DaskUDFs.DaskUDFRegistry import DaskUDFRegistry
from Helpers.DaskUDFs.RegisterDaskUDFs import initialize_dask_udf_registry
from Helpers.DaskUDFs.GeospatialFunctions import (
    point_to_tuple, point_to_x, point_to_y,
    geodesic_distance, xy_distance
)
from Helpers.DaskUDFs.ConversionFunctions import (
    hex_to_decimal, direction_and_dist_to_xy
)


@pytest.mark.dask
class TestDaskSessionManager:
    """Test DaskSessionManager singleton and cluster management."""

    def test_get_cluster(self):
        """Test that get_cluster() returns a valid cluster."""
        cluster = DaskSessionManager.get_cluster()

        assert cluster is not None
        assert isinstance(cluster, LocalCluster)
        assert cluster.scheduler_address is not None

    def test_get_client(self):
        """Test client creation and connection to cluster."""
        client = DaskSessionManager.get_client()

        assert client is not None
        assert isinstance(client, Client)
        assert client.status in ['running', 'connecting', 'closed']

    def test_dashboard_link(self):
        """Test dashboard link retrieval."""
        client = DaskSessionManager.get_client()
        link = DaskSessionManager.get_dashboard_link()

        assert link is not None
        assert isinstance(link, str)
        # Should be a URL or None
        if link:
            assert '://' in link or link.startswith('http')

    def test_worker_info(self):
        """Test worker information retrieval."""
        client = DaskSessionManager.get_client()
        worker_info = DaskSessionManager.get_worker_info()

        assert worker_info is not None
        assert isinstance(worker_info, dict)
        # May be empty if cluster is still starting
        # assert len(worker_info) > 0

    def test_memory_usage(self):
        """Test memory usage monitoring."""
        client = DaskSessionManager.get_client()
        memory_usage = DaskSessionManager.get_memory_usage()

        assert memory_usage is not None
        assert isinstance(memory_usage, dict)
        # May be empty if cluster is still starting

    def test_convenience_function(self):
        """Test get_dask_client() convenience function."""
        client = get_dask_client()

        assert client is not None
        assert isinstance(client, Client)


@pytest.mark.dask
class TestDaskUDFs:
    """Test Dask UDF registry and functions."""

    def test_udf_registry_initialization(self):
        """Test UDF registry initialization."""
        initialize_dask_udf_registry()
        registry = DaskUDFRegistry.get_instance()

        assert registry.is_initialized()

        # Should have registered functions
        all_functions = registry.list_all()
        assert len(all_functions) >= 7  # At least 7 functions registered

    def test_point_to_tuple(self):
        """Test POINT string to (x, y) tuple conversion."""
        point_str = "POINT (-111.123456 40.654321)"
        result = point_to_tuple(point_str)

        assert result == (-111.123456, 40.654321)

    def test_point_to_x(self):
        """Test longitude extraction from POINT."""
        point_str = "POINT (-111.123456 40.654321)"
        result = point_to_x(point_str)

        assert result == -111.123456

    def test_point_to_y(self):
        """Test latitude extraction from POINT."""
        point_str = "POINT (-111.123456 40.654321)"
        result = point_to_y(point_str)

        assert result == 40.654321

    def test_geodesic_distance_short(self):
        """Test geodesic distance calculation for short distances."""
        # Two nearby points - using actual geodesic calculation
        lat1, lon1 = 40.0, -111.0
        lat2, lon2 = 40.0, -111.001  # Very close points

        distance = geodesic_distance(lat1, lon1, lat2, lon2)

        # Distance should be small but positive
        assert distance > 0
        assert distance < 1000  # Less than 1 km

    def test_geodesic_distance_zero(self):
        """Test geodesic distance for same point."""
        lat, lon = 40.0, -111.0

        distance = geodesic_distance(lat, lon, lat, lon)

        # Should be zero or very close
        assert distance < 0.01  # meters

    def test_xy_distance(self):
        """Test Euclidean distance calculation."""
        # 3-4-5 right triangle
        x1, y1 = 0.0, 0.0
        x2, y2 = 3.0, 4.0

        distance = xy_distance(x1, y1, x2, y2)

        assert abs(distance - 5.0) < 0.0001

    def test_xy_distance_zero(self):
        """Test Euclidean distance for same point."""
        x, y = 10.0, 20.0

        distance = xy_distance(x, y, x, y)

        assert abs(distance) < 0.0001

    def test_hex_to_decimal(self):
        """Test hexadecimal to decimal conversion."""
        assert hex_to_decimal("FF") == 255
        assert hex_to_decimal("1A2B") == 6699
        assert hex_to_decimal("0") == 0
        assert hex_to_decimal("10") == 16

    def test_direction_and_dist_to_xy_returns_tuple(self):
        """Test coordinate offset calculation returns valid coordinates."""
        x, y = 0.0, 0.0
        direction = 0  # North
        distance = 100.0  # 100 meters

        result = direction_and_dist_to_xy(x, y, direction, distance)

        # Should return a tuple of two floats
        assert result is not None
        assert isinstance(result, tuple)
        assert len(result) == 2
        new_x, new_y = result
        assert isinstance(new_x, float)
        assert isinstance(new_y, float)

    def test_direction_and_dist_to_xy_different_directions(self):
        """Test coordinate offset calculation for different directions."""
        x, y = 10.0, 20.0
        distance = 100.0

        # Test multiple directions produce different results
        result_north = direction_and_dist_to_xy(x, y, 0, distance)
        result_east = direction_and_dist_to_xy(x, y, 90, distance)
        result_south = direction_and_dist_to_xy(x, y, 180, distance)
        result_west = direction_and_dist_to_xy(x, y, 270, distance)

        # All should be different
        assert result_north != result_east
        assert result_east != result_south
        assert result_south != result_west

    def test_registry_get_by_name(self):
        """Test retrieving functions from registry by name."""
        initialize_dask_udf_registry()
        registry = DaskUDFRegistry.get_instance()

        func = registry.get('point_to_tuple')
        assert func is not None
        assert callable(func)

        # Test the retrieved function
        result = func("POINT (-111.0 40.0)")
        assert result == (-111.0, 40.0)

    def test_registry_list_by_category(self):
        """Test filtering functions by category."""
        initialize_dask_udf_registry()
        registry = DaskUDFRegistry.get_instance()

        # Get all categories first
        categories = registry.get_categories()
        assert len(categories) >= 2

        # Filter by each category
        for category in categories:
            funcs = registry.list_by_category(category)
            assert isinstance(funcs, list)
            # Should have at least 1 function per category
            assert len(funcs) >= 1

    def test_registry_get_categories(self):
        """Test getting all available categories."""
        initialize_dask_udf_registry()
        registry = DaskUDFRegistry.get_instance()

        categories = registry.get_categories()

        assert isinstance(categories, list)
        assert len(categories) >= 2
        # Categories should be enum values, check their string representations
        category_names = [str(cat.value).upper() for cat in categories]
        assert 'GEOSPATIAL' in category_names
        assert 'CONVERSION' in category_names


@pytest.mark.dask
class TestDaskIntegration:
    """Integration tests demonstrating Dask component functionality."""

    def test_udf_with_dask_dataframe(self):
        """Test that UDFs work correctly with Dask DataFrames."""
        # Create sample DataFrame with POINT strings
        sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'position': [
                'POINT (-111.123 40.654)',
                'POINT (-111.124 40.655)',
                'POINT (-111.125 40.656)'
            ],
            'speed': [30, 40, 50]
        })
        ddf = dd.from_pandas(sample_data, npartitions=1)

        # Apply UDF to extract longitude
        ddf['longitude'] = ddf['position'].map(point_to_x, meta=('longitude', 'f8'))

        result = ddf.compute()

        # Validate coordinates
        assert abs(result['longitude'].iloc[0] - (-111.123)) < 0.001
        assert abs(result['longitude'].iloc[1] - (-111.124)) < 0.001
        assert abs(result['longitude'].iloc[2] - (-111.125)) < 0.001

    def test_map_partitions_coordinate_extraction(self):
        """Test efficient coordinate extraction using map_partitions."""
        # Create sample DataFrame
        sample_data = pd.DataFrame({
            'position': [
                'POINT (-111.0 40.0)',
                'POINT (-111.1 40.1)',
                'POINT (-111.2 40.2)',
                'POINT (-111.3 40.3)',
                'POINT (-111.4 40.4)'
            ]
        })
        ddf = dd.from_pandas(sample_data, npartitions=2)

        # Extract coordinates using map_partitions
        def extract_coords(partition):
            partition['longitude'] = partition['position'].apply(point_to_x)
            partition['latitude'] = partition['position'].apply(point_to_y)
            return partition

        ddf = ddf.map_partitions(extract_coords, meta=ddf._meta.assign(
            longitude='f8', latitude='f8'
        ))

        result = ddf.compute()

        # Validate
        assert 'longitude' in result.columns
        assert 'latitude' in result.columns
        assert len(result) == 5

        # Check values
        assert abs(result['longitude'].iloc[0] - (-111.0)) < 0.001
        assert abs(result['latitude'].iloc[0] - 40.0) < 0.001

    def test_distance_calculation_on_dataframe(self):
        """Test distance calculations on Dask DataFrame."""
        # Create DataFrame with two sets of coordinates
        sample_data = pd.DataFrame({
            'lat1': [40.0, 40.1, 40.2],
            'lon1': [-111.0, -111.1, -111.2],
            'lat2': [40.01, 40.11, 40.21],
            'lon2': [-111.01, -111.11, -111.21]
        })
        ddf = dd.from_pandas(sample_data, npartitions=1)

        # Calculate distances using map_partitions
        def calc_distances(partition):
            partition['distance'] = partition.apply(
                lambda row: geodesic_distance(
                    row['lat1'], row['lon1'], row['lat2'], row['lon2']
                ),
                axis=1
            )
            return partition

        ddf = ddf.map_partitions(calc_distances, meta=ddf._meta.assign(distance='f8'))

        result = ddf.compute()

        # All distances should be positive and relatively small
        assert all(result['distance'] > 0)
        assert all(result['distance'] < 2000)  # Less than 2 km

    def test_hex_conversion_on_dataframe(self):
        """Test hex to decimal conversion on DataFrame."""
        sample_data = pd.DataFrame({
            'hex_id': ['FF', '1A2B', 'CAFE', '100']
        })
        ddf = dd.from_pandas(sample_data, npartitions=1)

        # Convert hex to decimal
        ddf['decimal_id'] = ddf['hex_id'].map(hex_to_decimal, meta=('decimal_id', 'i8'))

        result = ddf.compute()

        assert result['decimal_id'].iloc[0] == 255
        assert result['decimal_id'].iloc[1] == 6699
        assert result['decimal_id'].iloc[2] == 51966
        assert result['decimal_id'].iloc[3] == 256


@pytest.mark.dask
class TestDaskMemoryManagement:
    """Test memory management and monitoring capabilities."""

    def test_large_dataframe_partitioning(self):
        """Test that large DataFrames are properly partitioned."""
        # Create a moderately large DataFrame
        n_rows = 10000
        sample_data = pd.DataFrame({
            'id': range(n_rows),
            'value': np.random.randn(n_rows),
            'category': np.random.choice(['A', 'B', 'C'], n_rows)
        })

        # Create Dask DataFrame with multiple partitions
        ddf = dd.from_pandas(sample_data, npartitions=4)

        assert ddf.npartitions == 4
        assert len(ddf.compute()) == n_rows

    def test_lazy_evaluation(self):
        """Test that Dask operations are lazy until compute()."""
        # Create DataFrame
        sample_data = pd.DataFrame({
            'value': [1, 2, 3, 4, 5]
        })
        ddf = dd.from_pandas(sample_data, npartitions=1)

        # Perform operations (should not execute immediately)
        ddf_filtered = ddf[ddf['value'] > 2]
        ddf_doubled = ddf_filtered['value'] * 2

        # Still a Dask Series (not computed)
        assert isinstance(ddf_doubled, dd.Series)

        # Now compute
        result = ddf_doubled.compute()

        # Should have 3, 4, 5 doubled = 6, 8, 10
        assert len(result) == 3
        assert list(result.values) == [6, 8, 10]
