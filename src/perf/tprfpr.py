import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

def my_confusion_matrix(y_pred, y_true, threshold):
    y_pred_class = y_pred > threshold
    y_true = y_true == 1
    cm = confusion_matrix(y_true, y_pred_class)
    # tn, fp, fn, tp = cm.ravel()
    return cm

def accuracy_score(conf_matrix):
    tn, fp, fn, tp = conf_matrix.ravel()

    return (tp + tn) / (tp + fp + fn + tn)

def f1_score(conf_matrix):
    tn, fp, fn, tp = conf_matrix.ravel()
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)

    f1_score = 2 * ( (precision * recall) / (precision + recall) )

    return f1_score

#  Legacy
# from sklearn.metrics import confusion_matrix
# from sklearn.preprocessing import Binarizer

# def my_conf_matrix(y_true, y_pred, threshold=.5):
    
#     y_pred = Binarizer(threshold=threshold).fit_transform(y_pred.reshape(-1,1))
    
#     # Confusion array
#     conf_array = confusion_matrix(y_true, y_pred)
    
#     # As data.frame
#     conf_frame = pd.DataFrame(
#         data = conf_array,
#         index = ['model_negative','model_positive'],
#         columns = ['target_negeative','target_positive']
#     )
    
#     return conf_frame