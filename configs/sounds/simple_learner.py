from src.learn.stackables import stackableLogisticRegression
from src.learn.stackables import stackableLGBMClassifier
import pandas as pd
import numpy as np

def build(target, train_features, X_pub, y_pub, my_skf):
    """
    A simple stack framework consisting of:
        * event_logr: Logistic regression for # days active per week features only
        * index_logr: Logistic regression over schedule match index features
        * demo_lgbm: LGBM model over demographic features
        * big_lgbm: Kitchen-sink model with all features thrown at it and reduced to the most performant features on the training set
        * cherry_logr: RFE optimised logistic regression over non-missing features from the LGBM optimised features
    """
    verbose=False

    # EVENT_LOGR  - logistic regression over 13-week event frequencies =====================
    event_logr = stackableLogisticRegression(
        name = 'event_logr',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if 'ew_' in f]
    )

    # INDEX_LOGR - logistic regression over schedule matching indices =====================
    index_logr = stackableLogisticRegression(
        name = 'index_logr',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if 'sched_match_index_' in f]
    )

    # BIG_LGBM - lightGBM (gradient-booster) over an optimised feature selection =====================
    big_lgbm = stackableLGBMClassifier(
        name = 'big_lgbm',
        target = target,
        features = [f for f in train_features], # to be optimised
        objective = 'binary',
        boosting_type = 'gbdt',
        num_leaves = 31,
        n_estimators = 100,
        subsample = .67,
        colsample_bytree = .55,
        n_jobs = -1,
        learning_rate = .1,
        silent = not verbose,
        importance_type = 'gain'
    )
    # Using a kitchen-sink model and select the features explaining most of the gain
    big_lgbm.optimise_features(X_pub, y_pub, my_skf, display_chart=False)

    # CHERRY_LOGR - logistic regression over rfe-optimised descriptive variables =====================
    cherry_logr = stackableLogisticRegression(
        name = 'cherry_logr',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features] # to be optimised
    )

    # Downsample X,y as RFE will struggle otherwise
    sample_size = 10000
    sample_size = min(sample_size, X_pub.shape[0])
    sample_idx = X_pub.sample(n = sample_size, replace = False).index
    X_sample = X_pub.loc[sample_idx]
    y_sample = y_pub.loc[sample_idx]

    # Start with the features used in the optimised big_lgbm
    # Columns with NA values won't work in a logistic regression,
    # so trimming those out
    F = big_lgbm.features
    na_counts = pd.DataFrame(data={
        'NAs':X_sample[F].apply(lambda x: len(X_sample[F])-x.count(), axis=0)
    })
    na_cols = na_counts[na_counts.NAs > 0].index.tolist()
    cherry_logr.features = [f for f in F if f not in na_cols]

    # Running RFE on to select the best features
    cherry_logr.rfe(X_sample, y_sample, n_features = 15)

    # DEMO_LGBM - lightGBM (gradient-booster) over demographic data =====================
    def demo_lgbm_feature(element):
        return 'acorn_' in element or \
               'gender_' in element or \
               'nation_' in element or \
               'barb_' in element or \
               'profile_age' == element or \
               'profile_age_1634_enriched' == element

    demo_lgbm = stackableLGBMClassifier(
        name = 'demo_lgbm',
        target = target,
        objective = 'binary',
        boosting_type = 'gbdt',
        num_leaves = 31,
        n_estimators = 100,
        n_jobs = -1,
        learning_rate = .1,
        silent = not verbose,
        importance_type = 'gain',
        features = [f for f in train_features if demo_lgbm_feature(f)]
    )

    # MODEL LIST =====================
    clfs = [
        event_logr,
        index_logr,
        cherry_logr,
        big_lgbm,
        demo_lgbm
    ]

    return clfs