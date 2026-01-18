"""
DaskMClassifierPipeline - Dask implementation of MClassifierPipeline.

This class manages the full ML training pipeline for BSM data with multiple sklearn classifiers.

Key differences from pandas version:
- Accepts Dask DataFrames (train_X, train_Y, test_X, test_Y)
- Automatically computes to pandas before passing to sklearn classifiers
- Delegates to MDataClassifier for actual training/testing (sklearn is pandas-only)
- Maintains same interface for results and confusion matrices

Usage:
    # Prepare data with Dask
    train_X_dask = cleaner.get_cleaned_data().drop('isAttacker', axis=1)
    train_Y_dask = cleaner.get_cleaned_data()['isAttacker']

    # Create pipeline (Dask DataFrames are auto-converted to pandas)
    pipeline = DaskMClassifierPipeline(train_X_dask, train_Y_dask, test_X_dask, test_Y_dask)

    # Train, test, and get results (same as pandas version)
    pipeline.train().test().calc_classifier_results()
    results = pipeline.get_classifier_results()

Notes:
    - sklearn classifiers require pandas DataFrames, so we compute() Dask DataFrames
    - Computing happens once during __init__ (not on every train/test call)
    - For large datasets (>2M rows), consider sampling for ML training
"""

from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
import dask.dataframe as dd
from dask.dataframe import DataFrame

from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Logger.Logger import Logger
from MachineLearning.MDataClassifier import MDataClassifier
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

DEFAULT_CLASSIFIER_INSTANCES = [
    RandomForestClassifier(),
    DecisionTreeClassifier(),
    KNeighborsClassifier()
]


class DaskMClassifierPipeline:
    """
    Dask-based ML classifier pipeline for training multiple sklearn models.

    This pipeline manages training and testing of multiple sklearn classifiers
    on BSM data. It handles Dask DataFrames by computing them to pandas before
    passing to sklearn (which only supports pandas).

    Supports:
        - Multiple sklearn classifiers (RandomForest, DecisionTree, KNeighbors)
        - Training and testing workflows
        - Metrics calculation (accuracy, precision, recall, F1, specificity)
        - Confusion matrix generation and plotting
    """

    @StandardDependencyInjection
    def __init__(self, train_X, train_Y, test_X, test_Y, pathprovider: IMLPathProvider, contextprovider: IMLContextProvider):
        """
        Initialize DaskMClassifierPipeline.

        Args:
            train_X (DataFrame or pd.DataFrame): Training features (Dask or pandas)
            train_Y (DataFrame or pd.Series): Training labels (Dask or pandas)
            test_X (DataFrame or pd.DataFrame): Test features (Dask or pandas)
            test_Y (DataFrame or pd.Series): Test labels (Dask or pandas)
            pathprovider (IMLPathProvider): Provides ML paths for outputs
            contextprovider (IMLContextProvider): Provides ML configuration
        """
        self._pathprovider = pathprovider()
        self._MLContextProvider = contextprovider()
        self.logger = Logger("DaskMClassifierPipeline")

        # Get classifier instances from configuration
        self.classifier_instances = self._MLContextProvider.get(
            "MClassifierPipeline.classifier_instances",
            DEFAULT_CLASSIFIER_INSTANCES
        )

        # Convert Dask DataFrames to pandas if needed
        # sklearn classifiers only support pandas DataFrames
        self.logger.log("Converting input data to pandas (if needed)...")

        # Check if inputs are Dask DataFrames and compute to pandas
        if isinstance(train_X, dd.DataFrame):
            self.logger.log("Computing train_X from Dask to pandas...")
            train_X = train_X.compute()

        if isinstance(train_Y, (dd.DataFrame, dd.Series)):
            self.logger.log("Computing train_Y from Dask to pandas...")
            train_Y = train_Y.compute()

        if isinstance(test_X, dd.DataFrame):
            self.logger.log("Computing test_X from Dask to pandas...")
            test_X = test_X.compute()

        if isinstance(test_Y, (dd.DataFrame, dd.Series)):
            self.logger.log("Computing test_Y from Dask to pandas...")
            test_Y = test_Y.compute()

        self.logger.log("Data conversion complete. Creating classifiers...")

        # Initialize results storage
        self.classifiers_and_confusion_matrices: list[tuple[MDataClassifier, list[list[float]]]] = []

        # Create MDataClassifier instances for each classifier
        # MDataClassifier is pandas-only, so we pass converted pandas data
        self.classifiers: list[MDataClassifier] = []
        for classifier_instance in self.classifier_instances:
            classifier_name = classifier_instance.__class__.__name__
            self.logger.log(f"Creating MDataClassifier for {classifier_name}...")
            self.classifiers.append(
                MDataClassifier(classifier_instance, train_X, train_Y, test_X, test_Y)
            )

        self.logger.log(f"Initialized {len(self.classifiers)} classifiers")

    def train(self):
        """
        Train all classifiers on training data.

        Returns:
            self: For method chaining
        """
        self.logger.log("Training all classifiers...")
        for mClassifier in self.classifiers:
            classifier_name = mClassifier.classifier.__class__.__name__
            self.logger.log(f"Training {classifier_name}...")
            mClassifier.train()
        self.logger.log("Training complete")
        return self

    def test(self):
        """
        Test all classifiers on both training and test data.

        Returns:
            self: For method chaining
        """
        self.logger.log("Testing all classifiers...")
        for mClassifier in self.classifiers:
            classifier_name = mClassifier.classifier.__class__.__name__
            self.logger.log(f"Testing {classifier_name}...")
            mClassifier.classify_train()
            mClassifier.classify()
        self.logger.log("Testing complete")
        return self

    def calc_classifier_results(self):
        """
        Calculate accuracy, precision, recall, F1, and specificity for each classifier.

        Returns:
            self: For method chaining
        """
        self.logger.log("Calculating classifier results...")
        self.results = []

        for mClassifier in self.classifiers:
            classifier_name = mClassifier.classifier.__class__.__name__
            self.logger.log(f"Calculating results for {classifier_name}...")
            train_results = mClassifier.get_train_results()
            test_results = mClassifier.get_results()
            self.results.append((mClassifier, train_results, test_results))

        self.logger.log("Results calculation complete")
        return self

    def get_classifier_results(self):
        """
        Get results for all classifiers.

        Returns:
            list[tuple]: List of (MDataClassifier, train_results, test_results)
                where train_results and test_results are tuples:
                (accuracy, precision, recall, f1, specificity)
        """
        return self.results

    def calculate_classifiers_and_confusion_matrices(self):
        """
        Calculate confusion matrices for all classifiers.

        Returns:
            self: For method chaining
        """
        self.logger.log("Calculating confusion matrices...")
        self.classifiers_and_confusion_matrices: list[tuple[MDataClassifier, list[list[float]]]] = []

        for mClassifier in self.classifiers:
            classifier_name = mClassifier.classifier.__class__.__name__
            self.logger.log(f"Calculating confusion matrix for {classifier_name}...")
            confusion_matrix = mClassifier.get_confusion_matrix()
            self.classifiers_and_confusion_matrices.append((mClassifier, confusion_matrix))

        self.logger.log("Confusion matrix calculation complete")
        return self

    def get_classifiers_and_confusion_matrices(self):
        """
        Get confusion matrices for all classifiers.

        Returns:
            list[tuple]: List of (MDataClassifier, confusion_matrix)
                where confusion_matrix is a 2D list of normalized values
        """
        return self.classifiers_and_confusion_matrices

    def plot_confusion_matrices(self):
        """
        Plot confusion matrices for all classifiers.

        Saves plots to paths specified by MLPathProvider and writes
        images to CSV output via ImageWriter.

        Returns:
            self: For method chaining
        """
        # Lazy import to avoid EasyMLLib dependency at module load time
        from Helpers.ImageWriter import ImageWriter

        self.logger.log("Plotting confusion matrices...")

        csvWriter = self._MLContextProvider.get("MClassifierPipeline.csvWriter")
        # write blank line
        imageWriter = ImageWriter(csvWriter)
        imageWriter.writeRow([])
        imageWriter.writeRow(["Classifier", "Confusion Matrix"])

        for mClassifier, confusion_matrix in self.classifiers_and_confusion_matrices:
            classifier_name = mClassifier.classifier.__class__.__name__
            self.logger.log(f"Plotting confusion matrix for {classifier_name}...")
            mClassifier.plot_confusion_matrix(confusion_matrix, classifier_name)

        self.logger.log("Confusion matrix plotting complete")
        return self
