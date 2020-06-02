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

    # EXPLORER_LGBM - lightGBM (gradient-booster) for variable importance exploration =====================
    def explorer_lgbm_feature(element):
        return 'streaming_time_13w' == element or \
               'device_iplayer_st_tv_perc' in element or \
               'lw_distinct_series' in element or \
               'series_finales' in element or \
               'avg_episode_repeats' in element or \
               'freq_seg_latest_iplayer_A.' in element or \
               'profile_age_1634_enriched' == element or \
               'profile_gender_enriched_female' == element or \
               'profile_nation_England' == element or \
               'profile_barb_region_London' == element or \
               'profile_acc_age_days' == element or \
               'profile_acorn_type_description_Socialising young renters' in element or \
               'profile_mailverified' == element or \
               'profile_enablepersonalisation' == element or \
               'sqrt_genre_share_comedy' == element or \
               'iplayer_activating_genre_Sport' in element or \
               'iplayer_fav_content_genre_Drama' in element or \
               'sched_match_index' == element or \
               'mkt_opted_in' == element or \
               'sqrt_mkt_email_opens_13w' == element or \
               'iplayer_programme_follows_13w' == element

    explorer_lgbm = stackableLGBMClassifier(
        name = 'explorer_lgbm',
        target = target,
        objective = 'binary',
        boosting_type = 'gbdt',
        num_leaves = 31,
        n_estimators = 100,
        subsample = .67,
        colsample_bytree = .55,
        n_jobs = -1,
        learning_rate = .1,
        silent = not verbose,
        importance_type = 'gain',
        features = [f for f in train_features if explorer_lgbm_feature(f)]
    )

    # BIG_MLP - Multi-layer Percpetron (MLP) Neural Network, large feature selection =====================
    from src.learn.stackables import stackableMLPClassifier

    big_mlp = stackableMLPClassifier(
        solver = 'adam',
        # alpha = 1e-5,
        hidden_layer_sizes = (13, 13, 13),
        random_state = 1,
        features = train_features
    )

    # MODEL LIST =====================
    clfs = [
        event_logr,
        index_logr,
        cherry_logr,
        big_lgbm,
        demo_lgbm,
        explorer_lgbm
    ]

    return clfs