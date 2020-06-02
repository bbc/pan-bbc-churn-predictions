# Admin things
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
from time import time
from datetime import date
import pickle
import re

# Techy things
import boto3
import os

# Number things
import pandas as pd
import numpy as np
import math
from scipy import interp

# Picture things
import matplotlib.pyplot as plt
import seaborn as sns

# Machine learning things
from sklearn import preprocessing
from sklearn import model_selection
from sklearn import linear_model

# My things
from src import utils
from src import fi

#MLOps
#import mlflow

# Directories
pickle_dir = 'pickles/iplayer'
log_dir = 'logs/iplayer'
charts_dir="charts/perf"

# Plot distributions of variables? Slow on large datasets
plot_distributions = False

# Fetch credentials from AWS
FEATURE_TRAINING_SET="../data/input/training/iplayer_training_set.csv"
df = pd.read_csv(FEATURE_TRAINING_SET)
print("Data loaded\n")


## =======================
## METADATA
## =======================

# FLAGGING FEATURES ==============

source_cols = df.columns.tolist()

id_vars = ['bbc_hid3',
           'destination',
           'target_week_start_date',
           'target_churn_this_week',
           'target_churn_next_week',
           'active_last_week',
           'train',
           'train_eligible',
           'cohort',
           'ew_0',
           'ew_1'
          ]
utils.pickler(id_vars, pickle_dir+'/prep/id_vars')

# Using naming conventions to pick out continuous variables
def continuous_feature(element):
    return 'genre_st_' in element or \
           'genre_share_' in element or \
           'releases_' in element or \
           'scaled_releases' in element or \
           'sched_match' in element or \
           ('iplayer_prod_' in element and 'preferred' not in element) or \
           ('device_' in element and 'preferred' not in element) or \
           'iplayer_activ_f' in element or \
           'iplayer_fav_f' in element or \
           'mkt_days_' in element or \
           'mkt_email_' in element or \
           'iplayer_tod_' in element or \
           'iplayer_dow_' in element

cont_vars = [
    'profile_age',
    'profile_acc_age_days',
    'streaming_time_13w'
    ] + \
    ['stw_'+str(x) for x in range(2,15)] + \
    ['ew_'+str(x) for x in range(2,15)] + \
    ['iplayer_lin_reg_coeff',
     'iplayer_13w_yintercept',
     'iplayer_13w_xintercept',
     'sounds_lin_reg_coeff',
     'sounds_13w_yintercept',
     'sounds_13w_xintercept'
    ] + \
    ['genre_distinct_count',
     'lw_distinct_series',
     'lw_distinct_episodes',
     'lw_series_premieres',
     'lw_series_finales',
     'lw_avg_episode_repeats',
     'iplayer_weeks_since_activation',
     'iplayer_programme_follows_13w',
     'iplayer_programme_follows_lastweek',
    ] + \
    [f for f in source_cols if continuous_feature(f)]
    
# Using naming conventions to pick out ordinal variables
def factor_feature(element):
    return ('device_' in element and 'preferred' in element) or \
           'freq_seg_latest_' in element
    
fac_vars = [
    'profile_enablepersonalisation',
    'profile_mailverified',
    'profile_nation',
    'profile_barb_region',
    'profile_acorn_type_description',
    'profile_acorn_group_description',
    'profile_acorn_category_description',
    'profile_age_1634_enriched',
    'profile_gender_enriched',
    'sounds_user',
    'sounds_active_last_week',
    'iplayer_lin_reg_churn_flag',
    'sounds_lin_reg_churn_flag',
    'lw_watched_finale_flag',
    'iplayer_activating_genre',
    'iplayer_activating_masterbrand',
    'sounds_fav_content_genre',
    'sounds_fav_content_masterbrand',
    'iplayer_fav_content_genre',
    'iplayer_fav_content_masterbrand',
    'mkt_opted_in'
    ] + \
    [f for f in source_cols if factor_feature(f)]

# No dates or timestamps in the dataset at the moment
date_vars = []

# Training feature list
load_features = fac_vars + cont_vars + date_vars

# Features that have been loaded but not flagged for purpose
# (could cause trouble later on)
naughty_list = [f for f in source_cols if f not in load_features + id_vars]



## =======================
## MISSING DATA
## =======================

from src.prep import missing_values

# Save list of emptys
s = df.apply(lambda x: x.count()==0)
empty_vars = s[s].index.tolist()

# NAs by column
na_counts = pd.DataFrame(data={'NAs':df.apply(lambda x: len(df)-x.count(), axis=0)})
na_counts.to_csv(log_dir+'/na_counts_%s.csv' % (date.today().strftime("%Y%m%d")))

impute_strategies = pd.DataFrame({
    'colname': ['profile_age', 'profile_nation', 'profile_barb_region', 'profile_acorn_type_description', 'profile_acorn_group_description', 'profile_acorn_category_description', 'profile_gender_enriched'],
    'strategy': ['regression', 'classification', 'classification', 'classification', 'classification', 'classification', 'classification', ]
})

helper_features = ['ew_'+str(x) for x in range(2,14)] + \
                  ['lw_distinct_series', 'lw_distinct_episodes'] + \
                  ['genre_share_drama', 'genre_share_comedy', 'genre_share_sport', 'genre_share_music']

# Create the missing value imputer to learn sensible substitute values
mvi = missing_values.missing_value_imputer(impute_strategies=impute_strategies,
                                           helper_features=helper_features)

# Train the imputer on df (the training set)
df_impute = mvi.train(df)

# Replacing missing value columns in fresh data with imputed columns
non_imputed = [c for c in df.columns if c not in mvi.impute_strategies.colname.values]
df = pd.concat([df[non_imputed], df_impute],  axis=1)

# Pickle the imputer for use with model scoring
utils.pickler(mvi, pickle_dir+'/prep/missing_value_imputer')

print("Missing value imputation completed\n")

# List of invariate columns
v = df[cont_vars].apply(lambda x: np.var(x), axis=0)
invariates = v[v==0].index.tolist()
utils.pickler(mvi, pickle_dir+'/prep/invariates')

print("Invariate features flagged\n")



## =======================
## TRANSFORMATIONS
## =======================

# ROW-WISE ===================
# Square-root transformations for lop-sided distributions:
# (Lazily using square-root here, come back and look at Box-Cox/Pick appropriate transformations later)
# Candidate variables for  transformation:
def sqrt_candidate(element):
    return 'genre_st_' in element or \
           'genre_share_' in element or \
           'ew_' in element or \
           ('device_' in element and 'perc' not in element) or \
           'mkt_email_' in element

sqrt_candidates = []#[f for f in cont_vars if sqrt_candidate(f)]

# Applying the square-root transformations
df_sqrt = df[sqrt_candidates].apply(func=np.sqrt,axis=0).rename(columns=lambda x: 'sqrt_'+x)
sqrt_vars = list(df_sqrt.columns.values)

# Pickle the sqrt candidates list for use on fresh data
utils.pickler(sqrt_candidates, pickle_dir+'/prep/sqrt_candidates')

print("Sqrt transformations completed\n")



# ONE-HOT ENCODING =======================
from src.prep import one_hot

oh_exceptions = [
    'mkt_opted_in',
    'profile_enablepersonalisation',
    'profile_mailverified',
    'profile_age_1634_enriched',
    'sounds_user',
    'sounds_active_last_week',
    'iplayer_lin_reg_churn_flag',
    'sounds_lin_reg_churn_flag',
    'lw_watched_finale_flag'
]

oh_candidates = [f for f in fac_vars if f not in oh_exceptions]
oh_encoder = one_hot.one_hot_encoder(oh_candidates)

# df_OH = pd.get_dummies(df[oh_candidates])
df_OH = oh_encoder.train(df)
oh_vars = oh_encoder.oh_vars

# Pickle the candidates
utils.pickler(oh_encoder, pickle_dir+'/prep/oh_encoder')

print("One-hot encoding completed\n")


# TIDY UP =======================
# Drop ineligible columns (missing, invariate or deprecated)
ineligibles = naughty_list + empty_vars + invariates + oh_candidates + sqrt_candidates
eligibles_untreated = [f for f in df.columns.tolist() if f not in ineligibles + id_vars]
utils.pickler(eligibles_untreated, pickle_dir+'/prep/eligibles_untreated')

# Drop deprecated / useless variables
df = pd.concat([df[id_vars + eligibles_untreated], df_sqrt, df_OH], axis=1)
df.set_index('bbc_hid3', inplace=True)

# Update eligible vars with new column names
eligibles = eligibles_untreated + df_sqrt.columns.tolist() + df_OH.columns.tolist()

# Filter to 'active last week', or relevant filter criteria for the model
df = df[(df.active_last_week == 1) & (df.train_eligible == 1)]

# Pickle
utils.pickler(eligibles, pickle_dir+'/prep/eligibles')



## =======================
## CONTROL
## =======================

# TARGET ==================
target = 'target_churn_next_week'

# Holdout seperated after feature treatment to remove bias. (We avoid data leakage from normalisers such as feature scaling by handling
# those steps within the training loop)
X_holdout, y_holdout = df[df.cohort == 0][eligibles], df[df.cohort == 0][[target]]

# Public/Private splits
from sklearn.model_selection import train_test_split
X_pub, X_priv, y_pub, y_priv = train_test_split(df[df.cohort != 0][eligibles], df[df.cohort != 0][[target]])
print('Train/Test Public/Private Sizes:')
for x in [X_priv, X_pub, y_priv, y_pub]: print(x.shape)
print('')

# Stratified K-Fold Cross-Validation ================
from sklearn.model_selection import StratifiedKFold

np.random.seed(10)
n_folds = 10

X, y = X_pub, y_pub
    
skf_model = StratifiedKFold(n_splits = n_folds, random_state=0)
splits = skf_model.split(X, y)

# Saving down the enumerator and indices from the SKF split for consistent reuse
my_skf = [[i, (train_idx, test_idx)] for i, (train_idx, test_idx) in enumerate(splits)]

print('Creating train / test splits over', n_folds, 'folds')
for i, (train_idx, test_idx) in my_skf:
    print(i,'= Train:', len(train_idx), 'Test:', len(test_idx))

# Suppression / Final feature selection================
all_cols = X_priv.columns.tolist()
suppressed_vars = []
train_features = [f for f in all_cols if f not in suppressed_vars]



## =======================
## MODEL SPECIFICATION
## =======================

#from configs.iplayer import simple_learner
#from configs.iplayer import lgbm_learner
from configs.iplayer import diverse_stack
#from configs.iplayer import logr_stack

# Simple exploratory / testing stack
# clfs = simple_learner.build(target, train_features, X_pub, y_pub, my_skf)
# clfs = lgbm_learner.build(target, train_features, X_pub, y_pub, my_skf)
clfs = diverse_stack.build(target, train_features, X_pub, y_pub, my_skf)
# clfs = logr_stack.build(target, train_features, X_pub, y_pub, my_skf, na_counts)



## =======================
## MODEL TRAINING
## =======================

from src.utils import scale_my_data
from src.learn import model_stacking

# Pass the clfs into a model stacker
stack = model_stacking.stacker(target, *clfs)

# Fitting the model stacker =====================
stack.fit(X_pub, y_pub, X_priv, y_priv, X_holdout, y_holdout, my_skf)

# Store the fitted model stack =====================
utils.pickler(stack, pickle_dir+'/models/stack')

# Checking the performance against the holdout dataset
y_holdout_predictions = stack.predict(X_holdout)

# # Print the log-loss against the out-of-time holdout
from sklearn.metrics import log_loss
holdout_logl = avg_logl = round(log_loss(y_holdout, y_holdout_predictions),2)
print('Stacked log-loss against out-of-time holdout: '+str(log_loss(y_holdout, y_holdout_predictions))) 

## =======================
## LOGISTIC REGRESSION
## =======================


## =======================
## PERFORMANCE AGAINST HOLDOUT
## =======================

from src.perf import roc, tprfpr

# ROC tracker for the model blender with 1 fold
holdout_ROC = roc.roc_cv('Stacker', n_folds = 1)
holdout_ROC.add_fold(y_holdout, y_holdout_predictions, label = 'Out-of-time ROC')

fig = plt.figure(figsize=(8,6))
ax = fig.add_subplot(1,1,1)
holdout_ROC.roc_plot()


fig.savefig(charts_dir+ "/lastrun_holdout_ROC.png")
print(os.getcwd())
print(os.listdir(os.getcwd()))


# Store ML interpretation Ingredients =====================
# little learners - feature importance
with open(pickle_dir+'/fi/little_fi.pickle', 'wb') as output_file:
    pickle.dump(stack.fi_dict, output_file)

    #MLflow save model

# little learners - roc performance
with open(pickle_dir+'/perf/little_roc.pickle', 'wb') as output_file:
    pickle.dump(stack.roc_dict, output_file)

    #MLflow save model little learner
    
# blender - feature importance
with open(pickle_dir+'/perf/blender_fi.pickle', 'wb') as output_file:
    pickle.dump(stack.coeff_values, output_file)

    #MLflow save model blender feature importance


# blender - roc performance
with open(pickle_dir+'/perf/stack_roc.pickle', 'wb') as output_file:
    pickle.dump(stack.roc, output_file)

    #MLflow save model blender

