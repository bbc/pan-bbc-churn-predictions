import psycopg2
import boto3
import os
import json
import pickle
import pdb
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeClassifier


def _impute_missing_values_worker(df, colname, strategy='regression', train=False):
    """
    Impute missing values in a pandas dataframe
    using lightGBM on present values
    """

    X, y = df.drop([colname], axis=1), df[colname].copy()

    X_train = X[df[colname].notnull()]

    pred_idx = X.index.difference(X_train.index)
    X_pred = X.loc[pred_idx]

    # Downsampling the training set if there's a large number of non-missing values:
    if (train and X_train.shape[0] > 100000):
        X_train = X_train.sample(n=100000, replace=True)

    y_train = y[X_train.index]


    if strategy == 'regression':
        imputer = LinearRegression()

    if strategy == 'classification':
        imputer = DecisionTreeClassifier()

    imputer.fit(X=X_train, y=y_train)
    predictions = imputer.predict(X_pred)

    y.loc[pred_idx] = predictions

    return y, imputer



def _score_missing_values_worker(df, colname, imputer):
    """
    Take a premade imputer and score on fresh missing values
    on a new dataset
    """

    X, y = df.drop([colname], axis=1), df[colname].copy()

    X_pred = X[df[colname].isnull()]
    pred_idx = X_pred.index

    predictions = imputer.predict(X_pred)

    y.loc[pred_idx] = predictions

    return y



class missing_value_imputer(object):
    """
    Using simple decision trees to make sensible substitutions for missing values
    """
    
    def __init__(self, impute_strategies, helper_features):
        """
        Creating a missing value imputer object

        -- Parameters --
        missing_value_strategies: dict
                keys: names of missing value columns
                values: one of 'regression' or 'classification' - strategy used to build the decision tree,
                        either continuous or categorical respectively.
        """
        # self.impute_strategies = pd.DataFrame({
        #     'colname': list(impute_strategies.keys()),
        #     'strategy': list(impute_strategies.values())
        # })
        self.impute_strategies = impute_strategies
        self.helper_features = helper_features

    def train(self, df):
        """

        """
        colnames = self.impute_strategies.colname
        strategies = self.impute_strategies.strategy

        df_impute = df[colnames].copy()

        imputers = []
        for i in range(0, len(colnames)):
            col = colnames[i]
            strategy = strategies[i]
    
            df_impute.loc[:,col], imputer = _impute_missing_values_worker(
                df[self.helper_features+[col]],
                col,
                strategy,
                train = True
            )
            imputers.append(imputer)

        self.imputers = imputers

        return df_impute

    def score(self, df):
        """

        """
        colnames = self.impute_strategies.colname
        strategies = self.impute_strategies.strategy

        df_impute = df[colnames].copy()

        for i in range(0, len(colnames)):
            col = colnames[i]
            strategy = strategies[i]
            imputer = self.imputers[i]

            df_impute.loc[:,col] = _score_missing_values_worker(
                df[self.helper_features+[col]],
                col,
                imputer
            )

        return df_impute
