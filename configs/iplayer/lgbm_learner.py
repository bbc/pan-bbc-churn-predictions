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
    event_lgbm = stackableLGBMClassifier(
        name = 'event_lgbm',
        target = target,
        features = [f for f in train_features if 'ew_' in f],
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

    # INDEX_LOGR - logistic regression over schedule matching indices =====================
    index_lgbm = stackableLGBMClassifier(
        name = 'index_lgbm',
        target = target,
        features = [f for f in train_features if 'sched_match_index_' in f],
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
    big_lgbm.optimise_features(X_pub, y_pub, my_skf, n_features=100, display_chart=False)

    # CHERRY_LOGR - logistic regression over rfe-optimised descriptive variables =====================
    little_lgbm = stackableLGBMClassifier(
        name = 'little_lgbm',
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
    little_lgbm.optimise_features(X_pub, y_pub, my_skf, n_features=30, display_chart=False)

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
        event_lgbm,
        index_lgbm,
        little_lgbm,
        big_lgbm,
        demo_lgbm
    ]

    return clfs