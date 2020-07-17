import numpy as np
import pytest
from src.perf import tprfpr


def test_my_confusion_matrix(tprfpr_inputs, tprfpr_confusion_matrix):
    expected = tprfpr_confusion_matrix

    result = tprfpr.my_confusion_matrix(tprfpr_inputs['y_pred'],
                                        tprfpr_inputs['y_true'],
                                        tprfpr_inputs['threshold'])

    np.testing.assert_array_equal(expected, result)


def test_accuracy_score(tprfpr_confusion_matrix):
    assert tprfpr.accuracy_score(tprfpr_confusion_matrix) == 0.6666666666666666


def test_f1_score(tprfpr_confusion_matrix):
    assert tprfpr.f1_score(tprfpr_confusion_matrix) == 0.4444444444444444


def test_accuracy_score_zeros(tprfpr_confusion_matrix_zeros):
    with pytest.warns(RuntimeWarning):
        tprfpr.f1_score(tprfpr_confusion_matrix_zeros) == 0.6
