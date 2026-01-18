"""
DaskConnectedDrivingAttacker - Dask implementation of ConnectedDrivingAttacker.

This class provides Dask-based attacker assignment for Connected Driving BSM datasets,
replacing pandas operations with distributed Dask DataFrame transformations.

Key differences from pandas version:
- Uses dask_ml.model_selection.train_test_split instead of sklearn
- Uses Dask DataFrame operations instead of pandas
- DataFrame operations return new DataFrames (no inplace=True)
- Supports both deterministic (by ID) and random attacker assignment

Compatibility with ConnectedDrivingAttacker:
- Follows same interface (IConnectedDrivingAttacker)
- Uses same configuration parameters (SEED, attack_ratio, isXYCoords)
- Produces identical attacker assignments (validated via golden datasets)

Usage:
    attacker = DaskConnectedDrivingAttacker(data=cleaned_df, id="attacker1")
    attacker.add_attackers()  # Deterministic by ID
    # OR
    attacker.add_rand_attackers()  # Random per row
    data_with_attackers = attacker.get_data()
"""

import random
import pandas as pd
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
    Dask-based attacker assignment for Connected Driving BSM data.

    This class assigns attacker labels to BSM records using either:
    1. Deterministic ID-based selection (add_attackers) - consistent across runs
    2. Random row-level selection (add_rand_attackers) - probabilistic

    The attacker assignment creates an 'isAttacker' column with values:
    - 0: Regular/benign vehicle
    - 1: Attacker vehicle

    Attack ratio is controlled via configuration (e.g., 0.05 = 5% attackers).
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
            # Fallback: use basic logging if Logger dependency fails
            import logging
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(f"DaskConnectedDrivingAttacker{id}")
            self.logger.log = self.logger.info  # Add .log() method for compatibility

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

        Note:
            This method computes the result to return a pandas Series for compatibility.
            Original pandas version returns df.coreData_id.unique().
        """
        # Dask pattern: use .unique().compute() to get pandas Series
        unique_ids = self.data['coreData_id'].unique().compute()
        self.logger.log(f"Found {len(unique_ids)} unique vehicle IDs")
        return unique_ids

    def add_attackers(self):
        """
        Add attacker labels using deterministic ID-based selection.

        This method:
        1. Extracts unique vehicle IDs
        2. Sorts IDs for consistent ordering (pandas.unique() vs dask.unique() order can differ)
        3. Splits IDs into regular vs attackers using train_test_split
        4. Adds 'isAttacker' column (0 for regular, 1 for attackers)

        The split is deterministic (controlled by SEED) and consistent across runs.

        Returns:
            self: For method chaining

        Note:
            Uses dask_ml.model_selection.train_test_split instead of sklearn.
            This approach ensures consistent attacker assignment across experiments.

            IMPORTANT: We sort unique IDs before splitting to ensure pandas and Dask
            versions produce identical results. pandas.unique() and dask.unique()
            can return values in different orders, and train_test_split is order-dependent.
        """
        self.logger.log(f"Adding attackers with attack_ratio={self.attack_ratio}, SEED={self.SEED}")

        # Get unique vehicle IDs (computed to pandas Series)
        uniqueIDs = self.getUniqueIDsFromCleanData()

        # CRITICAL: Sort IDs to ensure consistent order with pandas version
        # pandas.unique() and dask.unique() can return IDs in different orders
        # train_test_split is order-dependent, so sorting ensures identical results
        uniqueIDs_sorted = sorted(uniqueIDs)
        self.logger.log(f"Sorted {len(uniqueIDs_sorted)} unique IDs for deterministic splitting")

        # Split into regular vs attackers using dask-ml train_test_split
        # Note: train_test_split expects array-like
        regular, attackers = train_test_split(
            uniqueIDs_sorted,  # Use sorted IDs for consistency
            test_size=self.attack_ratio,
            random_state=self.SEED,
            shuffle=True  # Ensure reproducible shuffling
        )

        # Convert attackers to set for efficient lookup
        attackers_set = set(attackers)
        self.logger.log(f"Selected {len(attackers_set)} attackers out of {len(uniqueIDs)} total IDs")

        # Add isAttacker column using map_partitions for efficiency
        def _assign_attackers(partition):
            """Assign attacker labels within each partition."""
            partition['isAttacker'] = partition['coreData_id'].apply(
                lambda x: 1 if x in attackers_set else 0
            )
            return partition

        # Apply attacker assignment to all partitions
        meta = self.data._meta.copy()
        meta['isAttacker'] = 0  # Add isAttacker column to meta (dtype int64)

        self.data = self.data.map_partitions(_assign_attackers, meta=meta)

        self.logger.log("Attacker assignment complete")
        return self

    def add_rand_attackers(self):
        """
        Add attacker labels using random row-level selection.

        This method:
        1. Randomly assigns each row as attacker/regular based on attack_ratio
        2. Adds 'isAttacker' column (0 for regular, 1 for attackers)

        The assignment is probabilistic and NOT deterministic (even with SEED).
        Each row is independently assigned with probability = attack_ratio.

        Returns:
            self: For method chaining

        Note:
            Uses random.random() for each row, which may vary across partitions.
            For deterministic assignment, use add_attackers() instead.
        """
        self.logger.log(f"Adding random attackers with attack_ratio={self.attack_ratio}")

        # Set random seed for reproducibility
        random.seed(self.SEED)

        # Add isAttacker column using map_partitions
        def _assign_random_attackers(partition):
            """Randomly assign attacker labels within each partition."""
            # Set seed per partition for reproducibility
            random.seed(self.SEED)
            partition['isAttacker'] = partition['coreData_id'].apply(
                lambda x: 1 if random.random() <= self.attack_ratio else 0
            )
            return partition

        # Apply random attacker assignment to all partitions
        meta = self.data._meta.copy()
        meta['isAttacker'] = 0  # Add isAttacker column to meta (dtype int64)

        self.data = self.data.map_partitions(_assign_random_attackers, meta=meta)

        self.logger.log("Random attacker assignment complete")
        return self

    def get_data(self):
        """
        Retrieve the Dask DataFrame with attacker labels.

        Returns:
            DataFrame: Dask DataFrame with 'isAttacker' column added

        Note:
            Returns lazy Dask DataFrame. Call .compute() to materialize.
        """
        return self.data

    def add_attacks_positional_swap_rand(self):
        """
        Add position swap attack using compute-then-daskify strategy.

        This method implements Strategy 1 from Task 46 analysis:
        1. Compute Dask DataFrame to pandas
        2. Apply pandas position swap attack (reuses existing logic)
        3. Convert result back to Dask DataFrame

        Strategy 1 is recommended for 15-20M rows on 64GB system because:
        - Perfect compatibility (100% match with pandas version)
        - Memory-safe for target dataset sizes (18-48GB peak)
        - Reuses existing pandas attack code (zero logic changes)
        - Simplest to implement and validate

        The attack:
        - Only affects rows where isAttacker=1
        - For each attacker, randomly selects position from copydata
        - Swaps x_pos, y_pos, and coreData_elevation values
        - Uses .iloc[] for random row access (requires compute to pandas)

        Returns:
            self: For method chaining

        Note:
            This method materializes the entire DataFrame to pandas for the swap
            operation. Memory usage peaks at ~3x data size:
            - Original data (in memory)
            - Deep copy for swapping
            - Result DataFrame
            For 15-20M rows, peak usage is 18-48GB (within 52GB Dask limit).

        See Also:
            TASK_46_ILOC_ANALYSIS.md for detailed strategy analysis and validation.
        """
        self.logger.log("Starting position swap attack using compute-then-daskify strategy")

        # Get origin column names from context (for cache key compatibility)
        self.x_col_origin = self._generatorContextProvider.get("ConnectedDrivingCleaner.x_pos")
        self.y_col_origin = self._generatorContextProvider.get("ConnectedDrivingCleaner.y_pos")

        # Step 1: Compute Dask DataFrame to pandas
        self.logger.log("Computing Dask DataFrame to pandas for position swap...")
        df_pandas = self.data.compute()
        n_partitions = self.data.npartitions
        self.logger.log(f"Materialized {len(df_pandas)} rows from {n_partitions} partitions")

        # Step 2: Apply pandas position swap attack
        self.logger.log("Applying position swap attack to pandas DataFrame...")
        df_swapped = self._apply_pandas_position_swap_attack(df_pandas)

        # Step 3: Convert back to Dask DataFrame
        self.logger.log(f"Converting result back to Dask with {n_partitions} partitions...")
        self.data = dd.from_pandas(df_swapped, npartitions=n_partitions)

        self.logger.log("Position swap attack complete")
        return self

    def _apply_pandas_position_swap_attack(self, df_pandas):
        """
        Apply position swap attack to pandas DataFrame.

        This is the core attack logic, identical to StandardPositionalOffsetAttacker.
        For each attacker row:
        1. Pick a random row index from the entire dataset
        2. Copy x_pos, y_pos, and coreData_elevation from that random row
        3. Replace attacker's position with the random position

        Args:
            df_pandas (pd.DataFrame): Pandas DataFrame with isAttacker column

        Returns:
            pd.DataFrame: DataFrame with position-swapped attackers

        Note:
            This method MUST be applied to pandas DataFrame because it uses
            .iloc[] for random row access, which is NOT supported in Dask.
        """
        # Create deep copy for safe random position lookup
        copydata = df_pandas.copy(deep=True)
        self.logger.log(f"Created deep copy of {len(copydata)} rows for position swapping")

        # Set random seed for reproducibility
        random.seed(self.SEED)

        # Apply swap to each row (only affects attackers)
        df_swapped = df_pandas.apply(
            lambda row: self._positional_swap_rand_attack(row, copydata),
            axis=1
        )

        # Count attackers that were swapped
        n_attackers = (df_swapped['isAttacker'] == 1).sum()
        self.logger.log(f"Swapped positions for {n_attackers} attackers")

        return df_swapped

    def _positional_swap_rand_attack(self, row, copydata):
        """
        Swap position for a single attacker row.

        Args:
            row (pd.Series): Single row from DataFrame
            copydata (pd.DataFrame): Full dataset for random position lookup

        Returns:
            pd.Series: Row with position swapped (if attacker) or unchanged (if regular)

        Note:
            This method is identical to StandardPositionalOffsetAttacker.positional_swap_rand_attack
            to ensure 100% compatibility with pandas version.
        """
        # Only swap positions for attackers
        if row["isAttacker"] == 0:
            return row

        # Select random row index for position swap
        random_index = random.randint(0, len(copydata.index) - 1)

        # Swap based on coordinate system (XY or lat/lon)
        if self.isXYCoords:
            # Copy X, Y, and elevation from random row
            row[self.x_col] = copydata.iloc[random_index][self.x_col]
            row[self.y_col] = copydata.iloc[random_index][self.y_col]
            row["coreData_elevation"] = copydata.iloc[random_index]["coreData_elevation"]
        else:
            # Copy lat, lon, and elevation from random row
            row[self.pos_lat_col] = copydata.iloc[random_index][self.pos_lat_col]
            row[self.pos_long_col] = copydata.iloc[random_index][self.pos_long_col]
            row["coreData_elevation"] = copydata.iloc[random_index]["coreData_elevation"]

        return row

    def add_attacks_positional_offset_const(self, direction_angle=45, distance_meters=50):
        """
        Add constant positional offset attack using compute-then-daskify strategy.

        This method applies a constant positional offset to all attackers by:
        1. Computing Dask DataFrame to pandas
        2. Applying pandas positional offset attack (reuses existing logic)
        3. Converting result back to Dask DataFrame

        The attack:
        - Only affects rows where isAttacker=1
        - Offsets position by a constant direction and distance
        - Uses MathHelper for accurate offset calculations
        - Supports both XY coordinates and lat/lon coordinates

        Args:
            direction_angle (int/float): Direction in degrees (0° = North, positive = clockwise)
                Default: 45° (northeast)
            distance_meters (int/float): Distance to offset in meters
                Default: 50m

        Returns:
            self: For method chaining

        Note:
            This method materializes the entire DataFrame to pandas for the attack
            operation. Memory usage peaks at ~2x data size (original + result).
            For 15-20M rows, peak usage is 12-32GB (within 52GB Dask limit).

        Examples:
            # Apply 50m offset at 45° (northeast)
            attacker.add_attacks_positional_offset_const()

            # Apply 100m offset due north
            attacker.add_attacks_positional_offset_const(direction_angle=0, distance_meters=100)

            # Apply 200m offset due east
            attacker.add_attacks_positional_offset_const(direction_angle=90, distance_meters=200)
        """
        self.logger.log(
            f"Starting positional offset const attack: "
            f"direction={direction_angle}°, distance={distance_meters}m"
        )

        # Step 1: Compute Dask DataFrame to pandas
        self.logger.log("Computing Dask DataFrame to pandas for positional offset...")
        df_pandas = self.data.compute()
        n_partitions = self.data.npartitions
        self.logger.log(f"Materialized {len(df_pandas)} rows from {n_partitions} partitions")

        # Step 2: Apply pandas positional offset attack
        self.logger.log("Applying positional offset attack to pandas DataFrame...")
        df_offset = self._apply_pandas_positional_offset_const(df_pandas, direction_angle, distance_meters)

        # Step 3: Convert back to Dask DataFrame
        self.logger.log(f"Converting result back to Dask with {n_partitions} partitions...")
        self.data = dd.from_pandas(df_offset, npartitions=n_partitions)

        self.logger.log("Positional offset const attack complete")
        return self

    def _apply_pandas_positional_offset_const(self, df_pandas, direction_angle, distance_meters):
        """
        Apply constant positional offset attack to pandas DataFrame.

        This is the core attack logic, identical to StandardPositionalOffsetAttacker.
        For each attacker row:
        1. Calculate new position based on direction angle and distance
        2. Replace attacker's position with the new position

        Args:
            df_pandas (pd.DataFrame): Pandas DataFrame with isAttacker column
            direction_angle (int/float): Direction in degrees (0° = North, clockwise)
            distance_meters (int/float): Distance to offset in meters

        Returns:
            pd.DataFrame: DataFrame with position-offset attackers

        Note:
            This method applies the attack row-wise using pandas .apply().
            Only rows with isAttacker=1 are modified.
        """
        # Apply offset to each row (only affects attackers)
        df_offset = df_pandas.apply(
            lambda row: self._positional_offset_const_attack(row, direction_angle, distance_meters),
            axis=1
        )

        # Count attackers that were offset
        n_attackers = (df_offset['isAttacker'] == 1).sum()
        self.logger.log(f"Applied offset to {n_attackers} attackers")

        return df_offset

    def _positional_offset_const_attack(self, row, direction_angle, distance_meters):
        """
        Apply constant positional offset to a single attacker row.

        Args:
            row (pd.Series): Single row from DataFrame
            direction_angle (int/float): Direction in degrees (0° = North, clockwise)
            distance_meters (int/float): Distance to offset in meters

        Returns:
            pd.Series: Row with position offset (if attacker) or unchanged (if regular)

        Note:
            This method is identical to StandardPositionalOffsetAttacker.positional_offset_const_attack
            to ensure 100% compatibility with pandas version.
        """
        # Only offset positions for attackers
        if row["isAttacker"] == 0:
            return row

        # Calculate new position based on coordinate system (XY or lat/lon)
        if self.isXYCoords:
            # XY coordinate system (Cartesian)
            newX, newY = MathHelper.direction_and_dist_to_XY(
                row[self.x_col],
                row[self.y_col],
                direction_angle,
                distance_meters
            )
            row[self.x_col] = newX
            row[self.y_col] = newY
        else:
            # Lat/Lon coordinate system (WGS84 geodesic)
            newLat, newLong = MathHelper.direction_and_dist_to_lat_long_offset(
                row[self.pos_lat_col],
                row[self.pos_long_col],
                direction_angle,
                distance_meters
            )
            row[self.pos_lat_col] = newLat
            row[self.pos_long_col] = newLong

        return row
