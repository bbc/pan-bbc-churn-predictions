# # DEPRECATED - too slow and doesn't converge
# # Create the list of hyperparameters
# #     objective = 'binary',
# #     boosting_type = 'gbdt',
# #     num_leaves = 31,
# #     n_estimators = 100,
# #     n_jobs = -1,
# #     learning_rate = .1,
# #     silent = not verbose
# import random
# random.seed(10)

# X, y = X_pub, y_pub.iloc[:,0].values

# hps = {
# #     'learning_rate':[.02,0.1],
# #     'bagging_fraction':[0,1],
# #     'feature_fraction':[0,1],
#     'max_depth':range(1,20)
# #     'num_leaves':range(10,50)
# }

# big_lgbm = LGBMClassifier(
#     objective = 'binary',
#     boosting_type = 'gbdt',
#     num_leaves = 31,
#     n_estimators = 50,
#     n_jobs = -1,
#     silent = not verbose,
#     learning_rate = .1
# #     max_depth = 4
# )

# def lgbm_scoring(X, y):
#     big_lgbm.predict_proba(X)[:,1]
#     fpr, tpr, _ = roc_curve(y, y_score)
#     return auc(fpr, tpr)

# clf_features = feature_sets['big_lgbm']

# BOout = BayesianOptimisation(
#     hps = hps,
#     MLmodel = big_lgbm,
#     scoring_function = lgbm_scoring,
#     NpI = 10,
#     Niter = 50,
#     y_train = y,
#     X_train = X,
#     n_restarts = 10,
#     optim_rout = 'random_search',
#     xi = 0.0,
#     noise = 0.001
# ).optimise()

