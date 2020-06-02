from src.learn.stackables import stackableLogisticRegression
from src.learn.stackables import stackableLGBMClassifier
import pandas as pd
import numpy as np

def build(target, train_features, X_pub, y_pub, my_skf):
    """
    A simple stack framework consisting of:
        * Content: LGBM over user-content related variables, like genre affinity, schedule matching,
                   diversity of genres and TLEOs.
        * Behaviour: LGBM over behavioural features - how users consumed content (devices/time of day)
                    as well as recently starting or completing series
        * Marketing: LGBM over marketing features. Includes opt-ins/opt outs of markreting and
                    personalisation, clicks, and follows
        * Cross-sell:  LGBM over other products frequency segments. Exploring the cross-product impacts
                    into iplayer loyalty
        * 
    """
    verbose=False

    # CONTENT - LGBM over content-related features=====================
    def content_feature(element):
        return 'genre_distinct_count' in element or \
               'genre_share' in element or \
               'sched_match_index' in element

    content_lgbm = stackableLGBMClassifier(
        name = 'Content',
        target = target,
        features = [f for f in train_features if content_feature(f)],
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

    # BEHAVIOUR - LGBM over behavioural features ===========
    def behaviour_feature(element):
        return 'iplayer_device_count' in element or \
               ('device_iplayer_st_' in element and 'perc' in element) or \
               'device_iplayer_st_preferred' in element or \
               'iplayer_tod_' in element or \
               'iplayer_dow_' in element or \
               'lw_series_premieres' in element or \
               'lw_series_finales' in element or \
               'lw_distinct_series' in element or \
               'lw_distinct_episodes' in element
               

    behaviour_lgbm = stackableLGBMClassifier(
        name = 'Behaviour',
        target = target,
        features = [f for f in train_features if behaviour_feature(f)],
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

    # MARKETING - LGBM over behavioural features ===========
    def marketing_feature(element):
        return 'mkt_opted_in' in element or \
               'mkt_days_' in element or \
               'mkt_email_' in element or \
               'profile_enablepersonalisation' in element or \
               'profile_mailverified' in element
               

    marketing_lgbm = stackableLGBMClassifier(
        name = 'Marketing',
        target = target,
        features = [f for f in train_features if marketing_feature(f)],
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

    # CROSS-SELL - LGBM over behavioural features ===========
    def crosssell_feature(element):
        return ('freq_seg_' in element and 'iplayer' not in element and 'panbbc' not in element)

    crosssell_lgbm = stackableLGBMClassifier(
        name = 'Cross-Sell',
        target = target,
        features = [f for f in train_features if crosssell_feature(f)],
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

    # BROAD - LGBM over a wide variety of (actionable) features

        # MODEL LIST =====================
    clfs = [
        content_lgbm,
        behaviour_lgbm,
        marketing_lgbm,
        crosssell_lgbm
    ]

    return clfs