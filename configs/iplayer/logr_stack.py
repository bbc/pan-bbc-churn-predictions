from src.learn.stackables import stackableLogisticRegression
from src.learn.stackables import stackableLGBMClassifier
import pandas as pd
import numpy as np

def build(target, train_features, X_pub, y_pub, my_skf, na_counts):
    """
    A simple stack framework consisting of:
        * event_logr: Logistic regression for # days active per week features only
        * index_logr: Logistic regression over schedule match index features
        * demo_lgbm: LGBM model over demographic features
        * big_lgbm: Kitchen-sink model with all features thrown at it and reduced to the most performant features on the training set
        * cherry_logr: RFE optimised logistic regression over non-missing features from the LGBM optimised features
    """
    verbose=False

    def content_feature(element):
        return 'genre_distinct_count' in element or \
               'genre_share' in element or \
               'sched_match_index' in element or \
               'iplayer_fav_content_genre' in element or \
               'iplayer_fav_content_masterbrand' in element

    # EVENT_LOGR  - logistic regression over 13-week event frequencies =====================
    content_logr = stackableLogisticRegression(
        name = 'Content',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if content_feature(f)]
    )

    # BEHAVIOUR - LGBM over behavioural features ===========
    def behaviour_feature(element):
        return 'lw_series_premieres' in element or \
               'lw_series_finales' in element or \
               'lw_distinct_series' in element or \
               'lw_distinct_episodes' in element or \
               'iplayer_programme_follows_' in element
               
    behaviour_logr = stackableLogisticRegression(
        name = 'Behaviour',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if behaviour_feature(f)]
    )

    # MARKETING - LGBM over behavioural features ===========
    def marketing_feature(element):
        return 'mkt_opted_in' in element or \
               'mkt_email_' in element or \
               'profile_enablepersonalisation' in element or \
               'profile_mailverified' in element
               

    marketing_logr = stackableLogisticRegression(
        name = 'Marketing',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if marketing_feature(f)]
    )

        # CROSS-SELL - LGBM over behavioural features ===========
    def crosssell_feature(element):
        return ('freq_seg_' in element and 'iplayer' not in element and 'panbbc' not in element)

    crosssell_logr = stackableLogisticRegression(
        name = 'Cross-Sell',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if crosssell_feature(f)]
    )

    def rfe_feature(element):
        return (element in na_counts[na_counts['NAs']==0].index and \
            'ew_' not in element and
            'stw_' not in element and
            'streaming_time' not in element and
            'yintercept' not in element)

    rfe_logr = stackableLogisticRegression(
        name = 'RFE-Optimised',
        target = target,
        penalty = 'l2',
        fit_intercept = True,
        solver = 'sag',
        random_state = 0,
        max_iter = 200,
        verbose = verbose,
        n_jobs = -1,
        features = [f for f in train_features if rfe_feature(f)]
    ) 
    rfe_logr.rfe(X_pub, y_pub, n_features=30)

    rfe_lgbm = stackableLGBMClassifier(
        name = 'LGBM with RFE-Optimised Features',
        target = target,
        features = rfe_logr.features,
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

    # MODEL LIST =====================
    clfs = [
        content_logr,
        behaviour_logr,
        marketing_logr,
        crosssell_logr,
        rfe_logr,
        rfe_lgbm
    ]

    return clfs
