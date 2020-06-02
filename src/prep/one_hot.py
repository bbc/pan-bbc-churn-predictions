import psycopg2
import boto3
import os
import json
import pickle
import pdb
import pandas as pd

def _score_add_missing_columns(df, all_cols):
    missing_cols = set(all_cols) - set(df.columns)
    for c in missing_cols:
        df[c] = 0

def _score_repair_columns(df, all_cols):

    # Add missing columns
    _score_add_missing_columns(df, all_cols)
    assert(set(all_cols) - set(df.columns) == set())

    # Extra columns that will be removed (to be printed)
    extra_cols = set(df.columns) - set(all_cols)
    if extra_cols:
        print("extra columns: ",  extra_cols)

    df = df[all_cols]

    return df


class one_hot_encoder(object):
    """
    Encoder object to train and score an one hot encoder (using pandas dummies)
    with methods to handle new and missing columns in fresh data
    """

    def __init__(self, candidates):
        """
        Create an encoder for a list of variable names
        """
        self.candidates = candidates

    def train(self, df):
        """
        Create and "train" an encoder.

        N.B. We're not really training the encoder here, we're including it in a framework
        which captures column order and output columns so that we can consistently create
        the same encoded columns with fresh uses
        """
        df_OH = pd.get_dummies(df[self.candidates])
        oh_vars = list(df_OH.columns)

        self.oh_vars = oh_vars

        return df_OH

    def score(self, df):
        """
        Create one-hot encoded datasets for fresh data, with columns aligned to original
        training data
        """
        # Applying the dummies to new data
        df_OH = pd.get_dummies(df[self.candidates])
        new_oh_vars = list(df_OH.columns)

        df_OH = _score_repair_columns(df_OH, self.oh_vars)

        return df_OH
