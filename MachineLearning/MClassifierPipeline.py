from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Helpers.ImageWriter import ImageWriter
from Logger.Logger import Logger
from MachineLearning.MDataClassifier import MDataClassifier
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider

DEFAULT_CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

class MClassifierPipeline:
    @StandardDependencyInjection
    def __init__(self, train_X, train_Y, test_X, test_Y, pathprovider: IMLPathProvider, contextprovider: IMLContextProvider):
        self._pathprovider = pathprovider()
        self._MLContextProvider = contextprovider()
        self.logger = Logger("MClassifierPipeline")
        self.classifier_instances = self._MLContextProvider.get("MClassifierPipeline.classifier_instances", DEFAULT_CLASSIFIER_INSTANCES)

        self.classifiers_and_confusion_matrices: list[tuple[MDataClassifier, list[list[float]]]] = []

        # create MDataClassifier instances for each classifier
        self.classifiers: list[MDataClassifier] = []
        for classifier_instance in self.classifier_instances:
            self.classifiers.append(
                MDataClassifier(classifier_instance, train_X, train_Y, test_X, test_Y))


    def train(self):
        for mClassifier in self.classifiers:
            mClassifier.train()

    def test(self):
        for mClassifier in self.classifiers:
            mClassifier.classify_train()
            mClassifier.classify()

    # gets accuracy, precision, recall and f1 score for each classifier and associates it with the classifier
    def calc_classifier_results(self):
        self.results = []

        for mClassifier in self.classifiers:
            self.results.append((mClassifier, mClassifier.get_train_results(), mClassifier.get_results()))

        return self

    def get_classifier_results(self):
        return self.results

    def calculate_classifiers_and_confusion_matrices(self):
        self.classifiers_and_confusion_matrices: list[tuple[MDataClassifier, list[list[float]]]] = []

        for mClassifier in self.classifiers:
            self.classifiers_and_confusion_matrices.append((mClassifier, mClassifier.get_confusion_matrix()))

        return self

    def get_classifiers_and_confusion_matrices(self):
        return self.classifiers_and_confusion_matrices

    def plot_confusion_matrices(self):

        csvWriter = self._MLContextProvider.get("MClassifierPipeline.csvWriter")
        # write blank line
        imageWriter = ImageWriter(csvWriter)
        imageWriter.writeRow([])
        imageWriter.writeRow(["Classifier", "Confusion Matrix"])

        for mClassifier, confusion_matrix in self.classifiers_and_confusion_matrices:
            mClassifier.plot_confusion_matrix(confusion_matrix, mClassifier.classifier.__class__.__name__)
        return self
