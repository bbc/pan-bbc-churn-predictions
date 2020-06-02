# Admin things
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
from time import time
import pickle

# Techy things
from sqlalchemy import create_engine
import boto3
import os
import json
import s3fs

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
pickle_dir = 'pickles/sounds'

# Plot distributions of variables? Slow on large datasets
plot_distributions = False

# Fetch credentials from AWS
aws_creds = utils.aws_fetch_creds()
secret_dict = utils.aws_fetch_secret('users/alex_philpotts/live/credentials')
engine_str = 'postgresql://%s:%s@localhost:5439/redshiftdb' % (
    secret_dict['redshift_username'],
    secret_dict['redshift_password'])

engine = create_engine(engine_str)

pd.options.display.max_columns = None
query = """select * from central_insights_sandbox.ap_churn_sounds_score_sample"""
df = pd.read_sql_query(query, engine)

print("Data loaded\n")

# df = stage_df.copy()


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

# Filter to 'active last week', or relevant filter criteria for the model
df = df[df.active_last_week == 1]

target = 'target_churn_next_week'

X = df[['bbc_hid3', 'target_week_start_date']+eligibles]
X.set_index(['bbc_hid3', 'target_week_start_date'], inplace=True)

# MODEL SCORING ==============

stack = utils.unpickle(pickle_dir+'/models/stack')

y_predictions = stack.predict(X, save_weak_learner_predictions = True, save_shap_sample = True)

weak_learner_preds_wide = stack.weak_learner_predictions
weak_learner_preds_long = pd.melt(
    weak_learner_preds_wide.reset_index(), id_vars = ['bbc_hid3', 'target_week_start_date'], var_name = 'weak_learner', value_name='prediction'
)
weak_learner_classes_wide = stack.weak_learner_classifications
weak_learner_classes_long = pd.melt(
    weak_learner_classes_wide.reset_index(), id_vars = ['bbc_hid3', 'target_week_start_date'], var_name = 'weak_learner', value_name='classification'
)

# Retrieving class threshold from the stack dictionary
weak_learner_thresholds = pd.DataFrame(stack.weak_learner_thresholds.items(), columns = ['weak_learner', 'threshold'])
weak_learner_classes_long = weak_learner_classes_long.merge(weak_learner_thresholds, left_on='weak_learner', right_on='weak_learner')

 # PUSH SCORES TO REDSHIFT ==============
from datetime import datetime

optimal_threshold = stack.roc.mean_optimal_threshold
optimal_classification = y_predictions.copy()
optimal_classification[optimal_classification >= optimal_threshold] = 1
optimal_classification[optimal_classification < optimal_threshold] = 0
optimal_classification = optimal_classification.astype(int)



# Export meta-model scores to Redshift
df_export_meta = pd.DataFrame({
    'bbc_hid3': df['bbc_hid3'],
    'target_week_start_date': df['target_week_start_date'],
    'learner_type': 'meta-learner',
    'learner_name': 'sounds-meta',
    'predicted_probability': y_predictions,
    'optimal_threshold': optimal_threshold,
    'optimal_classification': optimal_classification
})

df_export_weak = pd.DataFrame({
    'bbc_hid3': weak_learner_preds_long['bbc_hid3'],
    'target_week_start_date': weak_learner_preds_long['target_week_start_date'],
    'learner_type': 'weak-learner',
    'learner_name': weak_learner_preds_long['weak_learner'],
    'predicted_probability': weak_learner_preds_long['prediction'],
    'optimal_threshold': weak_learner_classes_long['threshold'],
    'optimal_classification': weak_learner_classes_long['classification'].astype(int)
})

df_export = pd.concat([df_export_meta, df_export_weak])

df_export['score_datetime'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')


utils.rs_upload(df=df_export,
                s3_obj='s3://central-insights/philpa03/loyalty-propensity-scores/model-scores',
                tmp='./data/model-scores.csv',
                rs_table='central_insights_sandbox.ap_churn_sounds_propensity_scores',
                aws_creds=aws_creds, # to access AWS
                secret_dict=secret_dict, # to access Redshift
                truncate=True,
                csv=True,
                gzip=False
                )


# Export SHAP data to s3
for (model_name, shap_values) in stack.shap_sample_dict.items():

    ### Update the archive with scores for this date - WILL OVERWRITE IF THE DATA ISN'T UPDATED
    shap_values.reset_index(level=['bbc_hid3','target_week_start_date'], inplace=True)
    target_dates = shap_values.target_week_start_date.unique()

    # Add whole batch to the current folder in s3
    (shap_values
        .query('target_week_start_date == @target_dates[0]')
        .drop(['bbc_hid3', 'target_week_start_date'], axis=1)
        .to_csv('s3://central-insights/philpa03/loyalty/sounds/shap-values/current/{model}.csv'.format(model = model_name), index=True)
        )

    # Loop over dates and add filtered dataset to each folder
    for date in target_dates:
        t = pd.to_datetime(str(date)).strftime('%Y%m%d')

        # Filter and push to S3
        (shap_values
            .query('target_week_start_date == @target_dates[0]')
            .drop(['bbc_hid3', 'target_week_start_date'], axis=1)
            .to_csv('s3://central-insights/philpa03/loyalty/sounds/shap-values/{date}/{model}.csv'.format(date = t, model = model_name), index=True)
        )

# Export feature importances to S3
for (model_name, fi) in stack.fi_dict.items():

    # Average the feature importance across folds and push to S3
    (pd.DataFrame(fi.importances.mean(1), columns=['gain'])
        .rename_axis('feature')
        .to_csv('s3://central-insights/philpa03/loyalty/sounds/fi/{model}.csv'.format(model = model_name), index=True)
    )


    

# Pickle stack to make sure everything is working
utils.pickler(stack, pickle_dir+'/sample_stack.pickle')

# update_s3_model_scores('little_lgbm', feature_values, shap_values)