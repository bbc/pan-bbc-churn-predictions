# Extending classification models to include helpful attributes and methods for model stacking.

# Number things
import pandas as pd
import numpy as np
import math
from scipy import interp

# Machine learning things
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from lightgbm import LGBMClassifier
from sklearn.feature_selection import RFE

# My things
from src import utils
from src import fi

class stackableLogisticRegression():
    """
    Extending the logistic regression class to add feature sets
    and methods for RFE
    """
    # __metaclass__ = LogisticRegression
    # @classmethod
    # def cast(cls, clf: LogisticRegression, features, target, name):
    #     """
    #     Cast a LogistcRegression model into a stackableLogisticRegression Model
    #     """
    #     assert isinstance(clf, LogisticRegression)
    #     clf.__class__ = LogisticRegression
    #     # assert isinstance(clf, stackableLogisticRegression)

    #     clf.features = features
    #     clf.target = target
    #     clf.name = name
    #     clf.model = 'logr'

    #     return clf

    def __init__(self, features, target, name="", **kwargs):
        """
        Extending the LogisticRegression class from sklearn to include
        features
        """
        self.clf = LogisticRegression(**kwargs)

        self.features = features
        self.target = target
        self.name = name
        self.algorithm = 'logr'



    def rfe(self, X, y, n_features = 12, verbose = 0):
        """
        Recursive Feature Elimination optimisation for logistic regressions.
        Starting with a candidate list of variables and iteratively eliminating
        the least significant variable (lowest p-value for correlation) until
        a desired number of features is met
        """
        X = X.loc[:,self.features]
        y = y.iloc[:,0].values

        # Scaling the data (it will be scaled for the actual implementation, and sag l)
        X = utils.scale_my_data(X)

        selector = RFE(self.clf, n_features, step=1, verbose=verbose)
        selector = selector.fit(X, y)

        self.features = list(X.loc[:,selector.support_].columns)



class stackableLGBMClassifier():
    """
    Exttending the LGBMClassifier class from lightGBM to add
    feature sets and methods for feature selection
    """

    def __init__(self, features, target, name = "", **kwargs):
        """
        Extending the LGBMClassifier class from lightGBM to include
        features
        """
        self.clf = LGBMClassifier(**kwargs)

        self.features = features
        self.target = target
        self.name = name
        self.algorithm = 'lgbm'


    def optimise_features(self, X_pub, y_pub, my_skf, n_features = 30, display_chart=True):
        """
        Optimise the feature selection of an LGBM using the most predictive
        features from a kitchen-sink model against a given dataset.
        """
        candidate_features = self.features
        importance_M = np.zeros((len(candidate_features), len(my_skf)))

        print('Selecting best features across ', str(len(my_skf)), ' folds...\n')

        X = X_pub.loc[:,candidate_features]

        # Building a kitchen-sink model
        for i, (train_idx, test_idx) in my_skf:
            
            X, y = X_pub.copy(), y_pub.copy()

            # Preparing train/test datasets for this fold
            X_train = X.iloc[train_idx]
            # X_train = X.iloc[train_idx]
            y_train = y.iloc[train_idx].loc[:,self.target].values
            X_test = X.iloc[test_idx]
            # X_test = X.iloc[test_idx]
            y_test = y.iloc[test_idx].loc[:,self.target].values

            # Application of scaling
            X_train = utils.scale_my_data(X_train)
            X_test = utils.scale_my_data(X_test)

            # Fitting the classifiers
            self.clf.fit(X_train, y_train,
                    eval_set=[(X_test, y_test)],
                    early_stopping_rounds = 10,
                    verbose=False)

            # Extracting top features
            # print('Features:',self.feature_importances_.shape)
            importance_M[:,i] = self.clf.feature_importances_

            print('Fold ',i,' complete')

        print('\nDone\n')
        importance = importance_M.mean(1)
        top_idx = np.argsort(importance)[-1:-min(len(importance), n_features):-1]

        if display_chart:
            fi.my_featimp(importance, X.loc[:,candidate_features].columns, self.name, self.algorithm, n_show = 15)

        self.features = [candidate_features[i] for i in top_idx]

class stackableMLPClassifier():
    """
    Exttending the MLPClassifier class from sklearn to add
    feature sets and methods for feature selection
    """
    def __init__(self, features, **kwargs):
        """
        Extending the MLPClassifier class from sklearn to include
        features
        """
        self.clf = MLPClassifier(**kwargs)
        self.features = features
        self.algorithm = 'mlpnn'