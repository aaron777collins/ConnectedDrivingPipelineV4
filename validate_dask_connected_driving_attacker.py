"""
Validation script for DaskConnectedDrivingAttacker.

This script validates:
1. Initialization and dependency injection
2. getUniqueIDsFromCleanData() method
3. add_attackers() method (deterministic ID-based)
4. add_rand_attackers() method (random row-level)
5. Attack ratio accuracy
6. SEED determinism
7. Method chaining
8. Error handling
"""

import sys
import pandas as pd
import dask.dataframe as dd
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, '/tmp/original-repo')

from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.DaskSessionManager import DaskSessionManager


def create_mock_providers(attack_ratio=0.1, seed=42):
    """Create mock providers for testing."""
    # Mock path provider
    path_provider = MagicMock()
    path_provider.return_value = MagicMock()

    # Mock context provider with configuration
    context_provider = MagicMock()
    context_instance = MagicMock()
    context_instance.get = MagicMock(side_effect=lambda key, default=None: {
        "ConnectedDrivingAttacker.SEED": seed,
        "ConnectedDrivingCleaner.isXYCoords": False,
        "ConnectedDrivingAttacker.attack_ratio": attack_ratio,
    }.get(key, default))
    context_provider.return_value = context_instance

    return path_provider, context_provider


def create_sample_data(num_rows=1000, num_unique_ids=100):
    """
    Create sample cleaned BSM data for testing.

    Args:
        num_rows: Total number of rows
        num_unique_ids: Number of unique vehicle IDs

    Returns:
        Dask DataFrame with BSM-like structure
    """
    import numpy as np

    # Generate realistic BSM data
    np.random.seed(42)

    # Create unique IDs (cycling through num_unique_ids)
    unique_ids = [f"VEH_{i:04d}" for i in range(num_unique_ids)]
    core_data_ids = [unique_ids[i % num_unique_ids] for i in range(num_rows)]

    data = pd.DataFrame({
        'coreData_id': core_data_ids,
        'x_pos': np.random.uniform(-105.5, -104.5, num_rows),  # Colorado longitudes
        'y_pos': np.random.uniform(39.0, 40.0, num_rows),       # Colorado latitudes
        'speed': np.random.uniform(0, 100, num_rows),
        'heading': np.random.uniform(0, 360, num_rows),
    })

    # Convert to Dask DataFrame with 10 partitions
    dask_df = dd.from_pandas(data, npartitions=10)

    return dask_df


def test_initialization():
    """Test 1: Validate initialization and dependency injection."""
    print("\n" + "="*60)
    print("Test 1: Initialization and Dependency Injection")
    print("="*60)

    # Initialize Dask client
    client = DaskSessionManager.get_client()

    # Create sample data
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Create mock providers
    path_provider, context_provider = create_mock_providers(attack_ratio=0.1, seed=42)

    # Initialize attacker (dependency injection handles pathProvider and contextProvider)
    attacker = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_1"
    )

    # Validate attributes
    assert attacker.id == "test_attacker_1", "ID mismatch"
    assert attacker.SEED == 42, f"SEED mismatch: expected 42, got {attacker.SEED}"
    assert attacker.attack_ratio == 0.1, f"Attack ratio mismatch: expected 0.1, got {attacker.attack_ratio}"
    assert attacker.isXYCoords == False, "isXYCoords should be False"
    assert attacker.pos_lat_col == "y_pos", "pos_lat_col mismatch"
    assert attacker.pos_long_col == "x_pos", "pos_long_col mismatch"

    # Validate data type
    assert isinstance(attacker.data, dd.DataFrame), "Data should be Dask DataFrame"

    print(f"✓ Initialization successful")
    print(f"  - ID: {attacker.id}")
    print(f"  - SEED: {attacker.SEED}")
    print(f"  - Attack ratio: {attacker.attack_ratio}")
    print(f"  - Data type: {type(attacker.data)}")
    print(f"  - Data shape: {len(attacker.data)} rows (lazy)")


def test_get_unique_ids():
    """Test 2: Validate getUniqueIDsFromCleanData() method."""
    print("\n" + "="*60)
    print("Test 2: getUniqueIDsFromCleanData() Method")
    print("="*60)

    # Create sample data with known unique IDs
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Create mock providers
    path_provider, context_provider = create_mock_providers()

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_2",
        
        
    )

    # Get unique IDs
    unique_ids = attacker.getUniqueIDsFromCleanData()

    # Validate
    assert isinstance(unique_ids, (pd.Series, pd.Index)), "Should return pandas Series or Index"
    assert len(unique_ids) == 100, f"Expected 100 unique IDs, got {len(unique_ids)}"

    # Validate ID format
    for uid in unique_ids[:5]:
        assert uid.startswith("VEH_"), f"Invalid ID format: {uid}"

    print(f"✓ getUniqueIDsFromCleanData() successful")
    print(f"  - Unique IDs found: {len(unique_ids)}")
    print(f"  - Sample IDs: {unique_ids[:5].tolist()}")


def test_add_attackers():
    """Test 3: Validate add_attackers() method (deterministic ID-based)."""
    print("\n" + "="*60)
    print("Test 3: add_attackers() Method (Deterministic)")
    print("="*60)

    # Create sample data
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Create mock providers with 10% attack ratio
    path_provider, context_provider = create_mock_providers(attack_ratio=0.1, seed=42)

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_3",
        
        
    )

    # Add attackers
    result = attacker.add_attackers()

    # Validate method chaining
    assert result is attacker, "add_attackers() should return self for method chaining"

    # Compute data to validate
    data_with_attackers = attacker.get_data().compute()

    # Validate isAttacker column exists
    assert 'isAttacker' in data_with_attackers.columns, "isAttacker column missing"

    # Count attackers
    num_attackers = (data_with_attackers['isAttacker'] == 1).sum()
    num_regular = (data_with_attackers['isAttacker'] == 0).sum()
    total_rows = len(data_with_attackers)

    print(f"✓ add_attackers() successful")
    print(f"  - Total rows: {total_rows}")
    print(f"  - Attackers: {num_attackers} ({num_attackers/total_rows*100:.1f}%)")
    print(f"  - Regular: {num_regular} ({num_regular/total_rows*100:.1f}%)")

    # Validate attack ratio is approximately correct
    # With 100 unique IDs and 10% attack ratio, we expect ~10 attacker IDs
    unique_attacker_ids = data_with_attackers[data_with_attackers['isAttacker'] == 1]['coreData_id'].unique()
    print(f"  - Unique attacker IDs: {len(unique_attacker_ids)}")

    # With 10% attack ratio on 100 IDs, we expect ~10 unique attacker IDs
    assert 8 <= len(unique_attacker_ids) <= 12, \
        f"Expected ~10 unique attacker IDs, got {len(unique_attacker_ids)}"


def test_add_attackers_determinism():
    """Test 4: Validate SEED determinism for add_attackers()."""
    print("\n" + "="*60)
    print("Test 4: add_attackers() Determinism (SEED)")
    print("="*60)

    # Create sample data
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Run 1: SEED=42
    path_provider1, context_provider1 = create_mock_providers(attack_ratio=0.1, seed=42)
    attacker1 = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_4a",
        pathProvider=path_provider1,
        generatorContextProvider=context_provider1
    )
    attacker1.add_attackers()
    data1 = attacker1.get_data().compute()

    # Run 2: SEED=42 (same seed)
    sample_data2 = create_sample_data(num_rows=1000, num_unique_ids=100)  # Recreate to reset
    path_provider2, context_provider2 = create_mock_providers(attack_ratio=0.1, seed=42)
    attacker2 = DaskConnectedDrivingAttacker(
        data=sample_data2,
        id="test_attacker_4b",
        pathProvider=path_provider2,
        generatorContextProvider=context_provider2
    )
    attacker2.add_attackers()
    data2 = attacker2.get_data().compute()

    # Validate that attacker assignments are identical
    assert (data1['isAttacker'] == data2['isAttacker']).all(), \
        "Attacker assignments should be identical with same SEED"

    print(f"✓ Determinism validated")
    print(f"  - Run 1 attackers: {(data1['isAttacker'] == 1).sum()}")
    print(f"  - Run 2 attackers: {(data2['isAttacker'] == 1).sum()}")
    print(f"  - Assignments match: 100%")


def test_add_rand_attackers():
    """Test 5: Validate add_rand_attackers() method (random row-level)."""
    print("\n" + "="*60)
    print("Test 5: add_rand_attackers() Method (Random)")
    print("="*60)

    # Create sample data
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Create mock providers with 10% attack ratio
    path_provider, context_provider = create_mock_providers(attack_ratio=0.1, seed=42)

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_5",
        
        
    )

    # Add random attackers
    result = attacker.add_rand_attackers()

    # Validate method chaining
    assert result is attacker, "add_rand_attackers() should return self for method chaining"

    # Compute data to validate
    data_with_attackers = attacker.get_data().compute()

    # Validate isAttacker column exists
    assert 'isAttacker' in data_with_attackers.columns, "isAttacker column missing"

    # Count attackers
    num_attackers = (data_with_attackers['isAttacker'] == 1).sum()
    num_regular = (data_with_attackers['isAttacker'] == 0).sum()
    total_rows = len(data_with_attackers)

    print(f"✓ add_rand_attackers() successful")
    print(f"  - Total rows: {total_rows}")
    print(f"  - Attackers: {num_attackers} ({num_attackers/total_rows*100:.1f}%)")
    print(f"  - Regular: {num_regular} ({num_regular/total_rows*100:.1f}%)")

    # Validate attack ratio is approximately correct (within 20% tolerance)
    expected_attackers = total_rows * 0.1
    tolerance = expected_attackers * 0.2  # 20% tolerance
    assert abs(num_attackers - expected_attackers) <= tolerance, \
        f"Attack ratio off: expected ~{expected_attackers}, got {num_attackers}"


def test_error_handling():
    """Test 6: Validate error handling for invalid inputs."""
    print("\n" + "="*60)
    print("Test 6: Error Handling")
    print("="*60)

    # Test 1: Invalid data type (pandas DataFrame instead of Dask)
    try:
        path_provider, context_provider = create_mock_providers()
        pandas_df = pd.DataFrame({'coreData_id': ['VEH_0001', 'VEH_0002']})

        attacker = DaskConnectedDrivingAttacker(
            data=pandas_df,  # Wrong type!
            id="test_attacker_6",
            
            
        )
        assert False, "Should have raised TypeError for pandas DataFrame"
    except TypeError as e:
        print(f"✓ TypeError raised correctly for invalid data type")
        print(f"  - Error: {str(e)[:80]}...")


def test_get_data():
    """Test 7: Validate get_data() method."""
    print("\n" + "="*60)
    print("Test 7: get_data() Method")
    print("="*60)

    # Create sample data
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)

    # Create mock providers
    path_provider, context_provider = create_mock_providers()

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(
        data=sample_data,
        id="test_attacker_7",
        
        
    )

    # Add attackers
    attacker.add_attackers()

    # Get data
    data = attacker.get_data()

    # Validate
    assert isinstance(data, dd.DataFrame), "get_data() should return Dask DataFrame"
    assert 'isAttacker' in data.columns, "isAttacker column should exist"

    # Compute and validate
    data_computed = data.compute()
    assert len(data_computed) == 1000, "Row count should be preserved"

    print(f"✓ get_data() successful")
    print(f"  - Returns Dask DataFrame: True")
    print(f"  - Columns: {list(data.columns)}")
    print(f"  - Rows: {len(data_computed)}")


def main():
    """Run all validation tests."""
    print("="*60)
    print("DaskConnectedDrivingAttacker Validation Suite")
    print("="*60)

    # Initialize Dask session
    print("\nInitializing Dask session...")
    client = DaskSessionManager.get_client()
    print(f"✓ Dask session ready - Dashboard: {client.dashboard_link}")

    try:
        test_initialization()
        test_get_unique_ids()
        test_add_attackers()
        test_add_attackers_determinism()
        test_add_rand_attackers()
        test_error_handling()
        test_get_data()

        print("\n" + "="*60)
        print("ALL TESTS PASSED ✓")
        print("="*60)
        print("\nDaskConnectedDrivingAttacker is production-ready!")

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
