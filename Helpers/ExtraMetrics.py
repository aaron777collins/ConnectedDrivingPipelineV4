from sklearn.metrics import confusion_matrix


def specificity(true_labels, predicted_labels):
    """
    Computes the specificity of the predictions with respect to the true labels.
    :param true_labels: The true labels of the data.
    :param predicted_labels: The predicted labels of the data.
    :return: The specificity of the predictions with respect to the true labels.
    """
    tn, fp, fn, tp = confusion_matrix(true_labels, predicted_labels).ravel()
    return tn / (tn + fp)
