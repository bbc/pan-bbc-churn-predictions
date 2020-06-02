# Admin things
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
from time import time
import pickle
import copy

# Techy things
from sqlalchemy import create_engine
import boto3
import os
import json

# Number things
import pandas as pd
import numpy as np
import math
from scipy import interp

# Picture things
import matplotlib.pyplot as plt
import seaborn as sns
from jupyterthemes import jtplot
from IPython.display import display

# Machine learning things
from sklearn import preprocessing
from sklearn import model_selection
from sklearn import linear_model
import shap

# My things
from src import utils
from src import fi


# Directories
pickle_dir = 'pickles/iplayer'
log_dir = 'logs/iplayer'

# Plot distributions of variables? Slow on large datasets
plot_distributions = False
save_shap_sample = True

# Fetch credentials from AWS
aws_creds = utils.aws_fetch_creds()
secret_dict = utils.aws_fetch_secret('users/alex_philpotts/live/credentials')
engine_str = 'postgresql://%s:%s@localhost:5439/redshiftdb' % (
    secret_dict['redshift_username'],
    secret_dict['redshift_password'])

engine = create_engine(engine_str)

pd.options.display.max_columns = None
query = """select * from central_insights_sandbox.ap_churn_iplayer_score_sample"""
df = pd.read_sql_query(query, engine)

query = """select * from central_insights_sandbox.ap_churn_iplayer_score_sample_profiler"""
profiles = pd.read_sql_query(query, engine)

print("Data loaded\n")

# Filter to 'active last week', or relevant filter criteria for the model
df = df[df.active_last_week == 1]

## =======================
## TREATMENT
## =======================

# IMPUTING MISSING COLUMNS ==============

# Retrieving the missing value imputer
mvi = utils.unpickle(pickle_dir+'/prep/missing_value_imputer')

# Imputimg missing values on the fresh data
df_impute = mvi.score(df)

# Replacing missing value columns in fresh data with imputed columns
non_imputed = [c for c in df.columns if c not in mvi.impute_strategies.colname.values]
df = pd.concat([df[non_imputed], df_impute],  axis=1)

print("Missing value imputation completed\n")



# TRANSFORMATIONS ==============
#   --- copying the dataframe before treatment so we can average unencoded features
df_repr = copy.copy(df)

# Sqrt variables with heavily lopsided distributions
sqrt_candidates = utils.unpickle( pickle_dir+'/prep/sqrt_candidates')
df_sqrt = df[sqrt_candidates].apply(func=np.sqrt,axis=0).rename(columns=lambda x: 'sqrt_'+x)

print("Sqrt transformations completed\n")

# One-hot encoding
oh_encoder = utils.unpickle( pickle_dir+'/prep/oh_encoder')

df_OH = oh_encoder.score(df)

print("One-hot encoding completed\n")


# CONTROL ================

id_vars = utils.unpickle(pickle_dir+'/prep/id_vars')
eligibles_untreated = utils.unpickle(pickle_dir+'/prep/eligibles_untreated')
eligibles = utils.unpickle(pickle_dir+'/prep/eligibles')

# Drop deprecated / useless variables
df = pd.concat([df[id_vars + eligibles_untreated], df_sqrt, df_OH], axis=1)

target = 'target_churn_next_week'

X = df[['bbc_hid3', 'target_week_start_date']+eligibles]
X.set_index(['bbc_hid3', 'target_week_start_date'], inplace=True)
X.index.names = ['bbc_hid3', 'target_week_start_date']

# REPRESENTATIVE USERS ==============

# How we're building average users:
#  - Median of untreated / sqrted vars
#  - Mode of OH encoded vars (before OH encoding, then apply OH enconding and re-attach).
#        Tried just taking medians, but post-OH encoding this returns 0 for basically all categoricals,
#        so having to do it the long-winded way.

# Average users
median_cols = eligibles_untreated + ['sqrt_' + c for c in sqrt_candidates]
mode_cols = oh_encoder.candidates
unencoded_eligibles = median_cols + mode_cols

df_profiled = df_repr.merge(profiles, how='left', on='bbc_hid3')
users = dict(
    user_base = df_repr,
    user_1634 = df_profiled.query('age_1634 == 1'),
    user_1624 = df_profiled.query('age <= 24'),
    user_female = df_profiled.query('gender == "female"'),
    user_acorn_456 = df_profiled.query('acorn_cat_num == [4,5,6]')
)

def average_user(df, user_id):
    "Build an average user over a dataset"
    df_median = df[median_cols].median(axis=0)
    df_mode = df[mode_cols].mode(axis=0)
    df_out = pd.DataFrame({
        'user_id': user_id,
        'feature': unencoded_eligibles,
        'baseval': df_median.append(df_mode.iloc[0,])
    })

    return df_out

base_user = pd.concat(
    [average_user(data, key) for (key, data) in users.items()]
)


## BUILD MODIFIED USERS ==============

# Read modifications from csv
modifiers  = pd.read_csv('data/modifiers_average_user.csv', index_col=0)

# Cross join the base onto all modifiers (we want one modification per row)
modified_all = modifiers.merge(base_user, on='user_id', how='inner')
modified_all['newval'] = modified_all['baseval']

# Nudge or replace values depending on method column
modified_all.set_index(['user_id'], inplace=True)
delta_rows = (modified_all['method'] == 'delta') & (modified_all['feature'] == modified_all['mod_column'])
modified_all.loc[delta_rows, 'newval'] = modified_all.loc[delta_rows, 'baseval'] + \
    modified_all.loc[delta_rows, 'modifier'].astype(float)

replace_rows = (modified_all['method'] == 'replace') & (modified_all['feature'] == modified_all['mod_column'])
modified_all.loc[replace_rows, 'newval'] = modified_all.loc[replace_rows, 'modifier']

# Keep base values for reference later
base_vals = (
    modified_all.loc[modified_all['feature']==modified_all['mod_column'],]
    .drop(['method','modifier','newval','feature'], axis=1)
    .set_index(['mod_column'], append=True)
)

# Tidy up and index modified and base users
# -- Modified
modified_all.drop(['method','modifier','baseval'], axis=1, inplace=True)
modified_all = modified_all.set_index(['mod_column', 'feature'], append=True).unstack('feature')
modified_all.columns = modified_all.columns.droplevel()


# -- Base
# base_user['mod_column'] = 'none'
base_user = base_user.reset_index().set_index(['user_id', 'index']).drop('feature', axis=1).unstack()
base_user.columns = base_user.columns.droplevel()

# Apply encoding to modified and base users
def tidy_encode(df, num_vars, encoder):
    "Handling the encoding of categorical variables and converting numericals to floats"
    treated_df = pd.concat(
        [df[num_vars].astype(float),
         encoder.score(df[encoder.candidates])
        ],
    axis=1
    )
    return treated_df
base_user = tidy_encode(base_user, eligibles_untreated, oh_encoder)
modified_users = tidy_encode(modified_all, eligibles_untreated, oh_encoder)

# Fetch the stack from pickles
stack = utils.unpickle(pickle_dir+'/models/stack')

# Create predicted probabilities on base and modified users
base_prediction = pd.DataFrame({
    'base_prediction': stack.predict(base_user, save_weak_learner_predictions = True, save_shap_sample = save_shap_sample)
}, index = base_user.index)
modified_predictions = pd.DataFrame({
    'modified_prediction': stack.predict(modified_users, save_weak_learner_predictions = True, save_shap_sample = save_shap_sample)
}, index = modified_users.index)

modified_predictions = (modified_predictions
 .reset_index('mod_column')
 .merge(base_prediction, left_index=True, right_index=True)
 .set_index('mod_column', append=True)
 .merge(base_vals, left_index=True, right_index=True, how='left')
 .merge(modifiers.set_index('mod_column', append=True), left_index=True, right_index=True, how='left')
 .assign(modified_impact = lambda x: x.modified_prediction - x.base_prediction)
)[['baseval', 'modifier', 'method', 'base_prediction', 'modified_prediction', 'modified_impact']]

# modified_predictions
modified_predictions.to_excel('data/modified_feature_predictions.xlsx')
print(modified_predictions.head())
