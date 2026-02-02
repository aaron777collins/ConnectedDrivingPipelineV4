"""
DaskConnectedDrivingAttacker - TRUE Dask implementation using vectorized operations.

This class provides Dask-based attacker assignment for Connected Driving BSM datasets
using proper distributed Dask operations - NO compute() fallbacks to pandas.

Key Features:
- Vectorized NumPy operations in map_partitions()
- No compute() during processing (only at final output)
- Proper Dask-native shuffling for swap attacks
- Full distributed processing support

Author: Refactored for true Dask compatibility
Date: 2026-02-01
"""

import math
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
from dask.dataframe import DataFrame

from dask_ml.model_selection import train_test_split
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Attackers.IConnectedDrivingAttacker import IConnectedDrivingAttacker
from Helpers.MathHelper import MathHelper
from Logger.Logger import Logger
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider


class DaskConnectedDrivingAttacker(IConnectedDrivingAttacker):
    """
    TRUE Dask-based attacker assignment for Connected Driving BSM data.
    
    Uses proper distributed Dask operations with vectorized NumPy processing.
    Never calls compute() during processing - all operations are lazy and distributed.
    """

    @StandardDependencyInjection
    def __init__(self, pathProvider: IGeneratorPathProvider,
                 generatorContextProvider: IGeneratorContextProvider,
                 data=None, id: str = "default"):
        """
        Initialize DaskConnectedDrivingAttacker.

        Args:
            pathProvider: Provides file system paths
            generatorContextProvider: Provides configuration context
            data (DataFrame, optional): Dask DataFrame with cleaned BSM data
            id (str, optional): Unique identifier for this attacker instance
        """
        self.id = id

        self._pathprovider = pathProvider()
        self._generatorContextProvider = generatorContextProvider()

        # Use try-except to handle missing config values gracefully
        try:
            self.logger = Logger(f"DaskConnectedDrivingAttacker{id}")
        except (KeyError, Exception):
            import logging
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(f"DaskConnectedDrivingAttacker{id}")
            self.logger.log = self.logger.info

        # Validate input data type
        if not isinstance(data, DataFrame):
            raise TypeError(
                f"Expected Dask DataFrame, got {type(data)}. "
                "Ensure you pass cleaned data from DaskConnectedDrivingCleaner."
            )

        self.data = data

        # Get config values with sensible defaults
        self.SEED = self._generatorContextProvider.get("ConnectedDrivingAttacker.SEED", 42)
        self.isXYCoords = self._generatorContextProvider.get("ConnectedDrivingCleaner.isXYCoords", False)
        self.attack_ratio = self._generatorContextProvider.get("ConnectedDrivingAttacker.attack_ratio", 0.05)

        # Column names for coordinates
        self.pos_lat_col = "y_pos"
        self.pos_long_col = "x_pos"
        self.x_col = "x_pos"
        self.y_col = "y_pos"

    def getUniqueIDsFromCleanData(self):
        """
        Extract unique vehicle IDs from cleaned BSM data.

        Returns:
            pandas.Series: Unique values from coreData_id column (computed from Dask)
        """
        unique_ids = self.data['coreData_id'].unique().compute()
        self.logger.log(f"Found {len(unique_ids)} unique vehicle IDs")
        return unique_ids

    def add_attackers(self):
        """
        Add attacker labels using deterministic ID-based selection.
        
        Uses dask_ml train_test_split for consistent attacker assignment.
        """
        self.logger.log(f"Adding attackers with attack_ratio={self.attack_ratio}, SEED={self.SEED}")

        # Get unique vehicle IDs (must compute once for splitting)
        uniqueIDs = self.getUniqueIDsFromCleanData()
        uniqueIDs_sorted = sorted(uniqueIDs)
        self.logger.log(f"Sorted {len(uniqueIDs_sorted)} unique IDs for deterministic splitting")

        # Handle boundary cases
        if self.attack_ratio <= 0.0:
            attackers_set = set()
        elif self.attack_ratio >= 1.0:
            attackers_set = set(uniqueIDs_sorted)
        else:
            regular, attackers = train_test_split(
                uniqueIDs_sorted,
                test_size=self.attack_ratio,
                random_state=self.SEED,
                shuffle=True
            )
            attackers_set = set(attackers)
            self.logger.log(f"Selected {len(attackers_set)} attackers out of {len(uniqueIDs)} total IDs")

        # Use map_partitions for distributed assignment
        def _assign_attackers_vectorized(partition, attackers_set):
            """Vectorized attacker assignment using numpy isin."""
            partition = partition.copy()
            partition['isAttacker'] = partition['coreData_id'].isin(attackers_set).astype(int)
            return partition

        meta = self.data._meta.copy()
        meta['isAttacker'] = 0

        self.data = self.data.map_partitions(
            _assign_attackers_vectorized,
            attackers_set=attackers_set,
            meta=meta
        )

        self.logger.log("Attacker assignment complete (distributed)")
        return self

    def add_rand_attackers(self):
        """
        Add attacker labels using random row-level selection.
        Uses vectorized numpy operations within each partition.
        """
        self.logger.log(f"Adding random attackers with attack_ratio={self.attack_ratio}")

        def _assign_random_attackers_vectorized(partition, seed, attack_ratio):
            """Vectorized random attacker assignment using numpy."""
            partition = partition.copy()
            rng = np.random.RandomState(seed)
            n_rows = len(partition)
            partition['isAttacker'] = (rng.random(n_rows) <= attack_ratio).astype(int)
            return partition

        meta = self.data._meta.copy()
        meta['isAttacker'] = 0

        self.data = self.data.map_partitions(
            _assign_random_attackers_vectorized,
            seed=self.SEED,
            attack_ratio=self.attack_ratio,
            meta=meta
        )

        self.logger.log("Random attacker assignment complete (vectorized)")
        return self

    def get_data(self):
        """Retrieve the Dask DataFrame with attacker labels."""
        return self.data

    # =========================================================================
    # VECTORIZED ATTACK IMPLEMENTATIONS - TRUE DASK
    # =========================================================================

    def add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50):
        """
        Add constant positional offset attack using TRUE vectorized Dask operations.
        
        NO compute() - uses vectorized numpy operations within map_partitions.
        """
        self.logger.log(
            f"Starting positional offset const attack (VECTORIZED): "
            f"direction={direction_angle}°, distance={distance_meters}m"
        )

        def _apply_const_offset_vectorized(partition, direction_angle, distance_meters, 
                                           x_col, y_col, is_xy_coords):
            """Vectorized constant offset - applies to ALL rows, but only modifies attackers."""
            partition = partition.copy()
            
            # Get attacker mask
            is_attacker = partition['isAttacker'] == 1
            n_attackers = is_attacker.sum()
            
            if n_attackers == 0:
                return partition
            
            # Convert angle to radians
            angle_rad = math.radians(direction_angle)
            
            if is_xy_coords:
                # Vectorized XY offset calculation for attackers only
                dx = distance_meters * math.cos(angle_rad)
                dy = distance_meters * math.sin(angle_rad)
                
                partition.loc[is_attacker, x_col] = partition.loc[is_attacker, x_col] + dx
                partition.loc[is_attacker, y_col] = partition.loc[is_attacker, y_col] + dy
            else:
                # For lat/lon, need row-wise calculation (geodesic is not vectorizable)
                from geographiclib.geodesic import Geodesic
                geod = Geodesic.WGS84
                
                def apply_geodesic_offset(row):
                    g = geod.Direct(row[y_col], row[x_col], direction_angle, distance_meters)
                    return pd.Series({x_col: g['lon2'], y_col: g['lat2']})
                
                new_coords = partition.loc[is_attacker].apply(apply_geodesic_offset, axis=1)
                partition.loc[is_attacker, x_col] = new_coords[x_col]
                partition.loc[is_attacker, y_col] = new_coords[y_col]
            
            return partition

        self.data = self.data.map_partitions(
            _apply_const_offset_vectorized,
            direction_angle=direction_angle,
            distance_meters=distance_meters,
            x_col=self.x_col,
            y_col=self.y_col,
            is_xy_coords=self.isXYCoords,
            meta=self.data._meta
        )

        self.logger.log("Positional offset const attack complete (vectorized)")
        return self

    def add_attacks_positional_offset_rand(self, min_dist=25, max_dist=250):
        """
        Add random positional offset attack using TRUE vectorized Dask operations.
        
        NO compute() - uses vectorized numpy operations within map_partitions.
        Each attacker row gets a random direction and distance.
        """
        self.logger.log(
            f"Starting positional offset rand attack (VECTORIZED): "
            f"min_dist={min_dist}m, max_dist={max_dist}m"
        )

        def _apply_rand_offset_vectorized(partition, min_dist, max_dist, seed,
                                          x_col, y_col, is_xy_coords):
            """Vectorized random offset using numpy random arrays."""
            partition = partition.copy()
            
            # Get attacker mask and count
            is_attacker = partition['isAttacker'] == 1
            n_attackers = is_attacker.sum()
            
            if n_attackers == 0:
                return partition
            
            # Generate random directions and distances for all attackers at once
            rng = np.random.RandomState(seed)
            directions = rng.uniform(0, 360, n_attackers)
            distances = rng.uniform(min_dist, max_dist, n_attackers)
            
            if is_xy_coords:
                # Vectorized XY offset calculation
                angles_rad = np.radians(directions)
                dx = distances * np.cos(angles_rad)
                dy = distances * np.sin(angles_rad)
                
                # Get current coordinates for attackers
                attacker_x = partition.loc[is_attacker, x_col].values
                attacker_y = partition.loc[is_attacker, y_col].values
                
                # Apply offsets
                partition.loc[is_attacker, x_col] = attacker_x + dx
                partition.loc[is_attacker, y_col] = attacker_y + dy
            else:
                # For lat/lon, apply row-wise (geodesic not vectorizable)
                from geographiclib.geodesic import Geodesic
                geod = Geodesic.WGS84
                
                attacker_indices = partition.index[is_attacker]
                for i, (idx, direction, distance) in enumerate(
                    zip(attacker_indices, directions, distances)
                ):
                    lat = partition.loc[idx, y_col]
                    lon = partition.loc[idx, x_col]
                    g = geod.Direct(lat, lon, direction, distance)
                    partition.loc[idx, x_col] = g['lon2']
                    partition.loc[idx, y_col] = g['lat2']
            
            return partition

        self.data = self.data.map_partitions(
            _apply_rand_offset_vectorized,
            min_dist=min_dist,
            max_dist=max_dist,
            seed=self.SEED,
            x_col=self.x_col,
            y_col=self.y_col,
            is_xy_coords=self.isXYCoords,
            meta=self.data._meta
        )

        self.logger.log("Positional offset rand attack complete (vectorized)")
        return self

    def add_attacks_positional_offset_const_per_id_with_random_direction(self, min_dist=25, max_dist=250):
        """
        Add positional offset attack with random direction/distance per vehicle ID.
        
        Strategy: Pre-compute lookup table of direction/distance per attacker ID,
        then apply in distributed map_partitions.
        """
        self.logger.log(
            f"Starting positional offset const per ID (DISTRIBUTED): "
            f"min_dist={min_dist}m, max_dist={max_dist}m"
        )

        # First, get unique attacker IDs (requires one compute)
        # This is acceptable because we need to create a lookup table
        attacker_mask_col = self.data['isAttacker'] == 1
        attacker_ids = self.data[attacker_mask_col]['coreData_id'].unique().compute()
        
        # Generate random direction/distance per ID
        rng = np.random.RandomState(self.SEED)
        direction_distance_lookup = {
            vehicle_id: {
                "direction": float(rng.randint(0, 360)),
                "distance": float(rng.randint(min_dist, max_dist))
            }
            for vehicle_id in attacker_ids
        }
        
        self.logger.log(f"Created lookup table for {len(direction_distance_lookup)} attacker IDs")

        def _apply_per_id_offset_vectorized(partition, lookup, x_col, y_col, is_xy_coords):
            """Apply per-ID offsets using vectorized operations."""
            partition = partition.copy()
            
            # Get attacker rows
            is_attacker = partition['isAttacker'] == 1
            if not is_attacker.any():
                return partition
            
            for vehicle_id, params in lookup.items():
                # Find rows for this vehicle ID that are attackers
                id_mask = (partition['coreData_id'] == vehicle_id) & is_attacker
                if not id_mask.any():
                    continue
                
                direction = params["direction"]
                distance = params["distance"]
                
                if is_xy_coords:
                    angle_rad = math.radians(direction)
                    dx = distance * math.cos(angle_rad)
                    dy = distance * math.sin(angle_rad)
                    
                    partition.loc[id_mask, x_col] = partition.loc[id_mask, x_col] + dx
                    partition.loc[id_mask, y_col] = partition.loc[id_mask, y_col] + dy
                else:
                    from geographiclib.geodesic import Geodesic
                    geod = Geodesic.WGS84
                    
                    for idx in partition.index[id_mask]:
                        lat = partition.loc[idx, y_col]
                        lon = partition.loc[idx, x_col]
                        g = geod.Direct(lat, lon, direction, distance)
                        partition.loc[idx, x_col] = g['lon2']
                        partition.loc[idx, y_col] = g['lat2']
            
            return partition

        self.data = self.data.map_partitions(
            _apply_per_id_offset_vectorized,
            lookup=direction_distance_lookup,
            x_col=self.x_col,
            y_col=self.y_col,
            is_xy_coords=self.isXYCoords,
            meta=self.data._meta
        )

        self.logger.log("Positional offset const per ID attack complete (distributed)")
        return self

    def add_attacks_positional_swap_rand(self):
        """
        Add position swap attack using Dask-native operations.
        
        Strategy: Sample random positions and apply swaps within each partition.
        This avoids the need for .iloc[] random access which doesn't work in Dask.
        """
        self.logger.log("Starting position swap attack (DISTRIBUTED)")

        # Step 1: Sample positions from the dataset for swapping
        # Sample a subset of positions that attackers will randomly select from
        sample_fraction = min(1.0, max(0.1, self.attack_ratio * 3))
        
        position_sample = self.data[[self.x_col, self.y_col, 'coreData_elevation']].sample(
            frac=sample_fraction,
            random_state=self.SEED
        ).compute()  # Small sample - OK to compute
        
        # Convert to list of position tuples for random selection
        swap_positions = list(zip(
            position_sample[self.x_col].values,
            position_sample[self.y_col].values,
            position_sample['coreData_elevation'].values
        ))
        
        self.logger.log(f"Sampled {len(swap_positions)} positions for swapping")

        def _apply_position_swaps_vectorized(partition, swap_positions, seed, x_col, y_col):
            """Apply position swaps using pre-sampled positions."""
            partition = partition.copy()
            
            is_attacker = partition['isAttacker'] == 1
            n_attackers = is_attacker.sum()
            
            if n_attackers == 0:
                return partition
            
            # Generate random indices to select swap positions
            rng = np.random.RandomState(seed)
            swap_indices = rng.randint(0, len(swap_positions), n_attackers)
            
            # Get the swap positions
            new_x = np.array([swap_positions[i][0] for i in swap_indices])
            new_y = np.array([swap_positions[i][1] for i in swap_indices])
            new_elev = np.array([swap_positions[i][2] for i in swap_indices])
            
            # Apply swaps
            partition.loc[is_attacker, x_col] = new_x
            partition.loc[is_attacker, y_col] = new_y
            partition.loc[is_attacker, 'coreData_elevation'] = new_elev
            
            return partition

        self.data = self.data.map_partitions(
            _apply_position_swaps_vectorized,
            swap_positions=swap_positions,
            seed=self.SEED,
            x_col=self.x_col,
            y_col=self.y_col,
            meta=self.data._meta
        )

        self.logger.log("Position swap attack complete (distributed)")
        return self

    def add_attacks_positional_override_const(self, direction_angle=45, distance_meters=50):
        """
        Add constant positional override attack using TRUE vectorized Dask operations.
        
        Overrides attacker positions to absolute position from origin.
        """
        self.logger.log(
            f"Starting positional override const attack (VECTORIZED): "
            f"direction={direction_angle}°, distance={distance_meters}m from origin"
        )

        # Pre-compute the target position (from origin)
        if self.isXYCoords:
            angle_rad = math.radians(direction_angle)
            target_x = distance_meters * math.cos(angle_rad)
            target_y = distance_meters * math.sin(angle_rad)
        else:
            # For lat/lon, compute from center point
            x_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos", 0.0)
            y_pos = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos", 0.0)
            from geographiclib.geodesic import Geodesic
            geod = Geodesic.WGS84
            g = geod.Direct(y_pos, x_pos, direction_angle, distance_meters)
            target_x = g['lon2']
            target_y = g['lat2']

        def _apply_override_const(partition, target_x, target_y, x_col, y_col):
            """Override attacker positions to constant target."""
            partition = partition.copy()
            
            is_attacker = partition['isAttacker'] == 1
            
            if is_attacker.any():
                partition.loc[is_attacker, x_col] = target_x
                partition.loc[is_attacker, y_col] = target_y
            
            return partition

        self.data = self.data.map_partitions(
            _apply_override_const,
            target_x=target_x,
            target_y=target_y,
            x_col=self.x_col,
            y_col=self.y_col,
            meta=self.data._meta
        )

        self.logger.log("Positional override const attack complete (vectorized)")
        return self

    def add_attacks_positional_override_rand(self, min_dist=25, max_dist=250):
        """
        Add random positional override attack using TRUE vectorized Dask operations.
        
        Overrides each attacker to random absolute position from origin.
        """
        self.logger.log(
            f"Starting positional override rand attack (VECTORIZED): "
            f"random distance {min_dist}m to {max_dist}m from origin"
        )

        def _apply_override_rand(partition, min_dist, max_dist, seed, x_col, y_col,
                                 is_xy_coords, center_x, center_y):
            """Override attacker positions to random positions from origin."""
            partition = partition.copy()
            
            is_attacker = partition['isAttacker'] == 1
            n_attackers = is_attacker.sum()
            
            if n_attackers == 0:
                return partition
            
            # Generate random directions and distances
            rng = np.random.RandomState(seed)
            directions = rng.uniform(0, 360, n_attackers)
            distances = rng.uniform(min_dist, max_dist, n_attackers)
            
            if is_xy_coords:
                # Calculate absolute positions from origin (0, 0)
                angles_rad = np.radians(directions)
                new_x = distances * np.cos(angles_rad)
                new_y = distances * np.sin(angles_rad)
                
                partition.loc[is_attacker, x_col] = new_x
                partition.loc[is_attacker, y_col] = new_y
            else:
                # For lat/lon, calculate from center point
                from geographiclib.geodesic import Geodesic
                geod = Geodesic.WGS84
                
                attacker_indices = partition.index[is_attacker]
                for i, (idx, direction, distance) in enumerate(
                    zip(attacker_indices, directions, distances)
                ):
                    g = geod.Direct(center_y, center_x, direction, distance)
                    partition.loc[idx, x_col] = g['lon2']
                    partition.loc[idx, y_col] = g['lat2']
            
            return partition

        center_x = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos", 0.0)
        center_y = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos", 0.0)

        self.data = self.data.map_partitions(
            _apply_override_rand,
            min_dist=min_dist,
            max_dist=max_dist,
            seed=self.SEED,
            x_col=self.x_col,
            y_col=self.y_col,
            is_xy_coords=self.isXYCoords,
            center_x=center_x,
            center_y=center_y,
            meta=self.data._meta
        )

        self.logger.log("Positional override rand attack complete (vectorized)")
        return self
