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
import dask.dataframe as dd
from dask.dataframe import DataFrame

from dask_ml.model_selection import train_test_split
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Generator.Attackers.IConnectedDrivingAttacker import IConnectedDrivingAttacker
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
        2. Splits IDs into regular vs attackers using train_test_split
        3. Adds 'isAttacker' column (0 for regular, 1 for attackers)

        The split is deterministic (controlled by SEED) and consistent across runs.

        Returns:
            self: For method chaining

        Note:
            Uses dask_ml.model_selection.train_test_split instead of sklearn.
            This approach ensures consistent attacker assignment across experiments.
        """
        self.logger.log(f"Adding attackers with attack_ratio={self.attack_ratio}, SEED={self.SEED}")

        # Get unique vehicle IDs (computed to pandas Series)
        uniqueIDs = self.getUniqueIDsFromCleanData()

        # Split into regular vs attackers using dask-ml train_test_split
        # Note: train_test_split expects array-like, so we convert to list
        regular, attackers = train_test_split(
            uniqueIDs,
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
