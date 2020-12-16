# Admin things
import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)
# warnings.simplefilter(action='ignore', category=UserWarning)
from time import time, sleep
from copy import deepcopy

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
from sklearn.linear_model import LogisticRegression
import shap
from sklearn.metrics import log_loss
from lightgbm import LGBMClassifier

# My things
from src import utils
from src import fi

from src.perf import roc, tprfpr

# Defining the stackable models class.

def scale_my_data(X):
    scaler = preprocessing.MaxAbsScaler()
    X_scaled = scaler.fit_transform(X.values)
    X_scaled = pd.DataFrame(X_scaled, index=X.index, columns=X.columns)
    return X_scaled


class stacker(object):
    """
    Ensembling with model stacking using scikit-learn, inspired
    by MLWave's excellent ensembling guide:

    https://mlwave.com/kaggle-ensembling-guide/

    ---  Parameters ---
    *args: stackable_model(s)
         Models created using stackables framework (an extension
         of sklearn model objects that includes metadata such as applicable
         feature subsets)
    SHAP_sample_size: integer
        Maximum number of samples to take from each model when creating SHAP
        (feature importance) plots;
    
    -- Attributes --
    fi_dict: dict
        Dictionary of feature importance attributes for each weak learner.
        * Logistic regressions return coefficients as feature importance
        * GBMs return SHAP values as feature importance
    roc_dict: dict
        Dictionary of ROC data/plotting methods for each weak learner
    cm_dict: dict
        Dictionary of confusion matrices for each weak learner

    -- Methods --
    fit:
        (sequentially) fits each of the models (weak learners) using stratified k-fold cross-validation,
        and fits a stacked meta-learner using the predictions of each weak learner as features in the
        meta-learner.
        For more information on the methodology see: https://www.dropbox.com/s/at7klosa7tk49dq/Ensembling%20with%20Model%20Stacking%20-%20v1.pptx?dl=0

    weak_learner_coeffs:
        Prints a plot using the coefficients of each model for in the (logistic regression) meta-learner.

    stacked_roc:
        Prints an ROC plot for the meta-learner.
    """
    def __init__(self, target, *args, SHAP_sample_size = 1000):
        """
        Initialising a model stacker with a collection of classifiers created using
        stackable_model
        """
        clfs = {}
        for c in args:
            clfs[c.name] = c #(c.model, clf.features, clf.algorithm)

        self.clfs = clfs

        # Storage for the training loop
        self.clf_names = list(clfs.keys())

        # PERFORMANCE DICTIONARIES
        self.fi_dict = {}
        self.roc_dict = {}
        self.cm_dict = {}
        self.target = target

        self.SHAP_sample_size = SHAP_sample_size

        self.fitted = False

    def fit(self, X_pub, y_pub, X_priv, y_priv, X_holdout, y_holdout, skf, follow=False, plot_probability_dists=False):
        """
        Fit stacked models to X_pub / y_pub training datasets, using
        X_priv and y_priv to track performance with sampled-out data,
        and X_holdout, y_holdout to track out of time performance.

        --- Parameters ---
        X_pub: pandas DataFrame
            Features for the training set
        y_pub: array
            Target for the training set
        X_priv: pandas DataFrame
            Sampled out feature set for performance tracking
        y_priv: array
            Sampled out target set for performance tracking
        X_holdout: pandas DataFrame
            Out-of-time feature set to simulate predictive performance
            on data from next week
        y_holdout: array
            Out-of-time targets.
        skf: array
            Index framework for stratified K-fold
        """
        clfs = self.clfs
        clf_names = self.clf_names 
        fi_dict = self.fi_dict
        roc_dict = self.roc_dict
        cm_dict = self.cm_dict
        weak_learner_thresholds = {}

        target = self.target

        SHAP_sample_size = self.SHAP_sample_size

        # Finding the class balance for the public target, which we can use as a default threshold
        pub_size = y_pub.agg('count')[0]; print('Public dataset size: '+str(pub_size))
        class1 = y_pub.groupby([target]).agg({target:['count']}).loc[1][0]; print('Target class size in public dataset: '+str(class1))

        class_balance =  class1 / pub_size
        print('Public target class density: '+str(class_balance))

        X, y = X_pub, y_pub

        # Instantiate numeric arrays for what will become the train/test sets for the stacker model - to be filled in later
        train_blend = np.zeros((X.shape[0], len(clfs)))
        test_blend = np.zeros((X_priv.shape[0], len(clfs)))
        holdout_blend = np.zeros((X_holdout.shape[0], len(clfs)))

        # Instantiate a dictionary for storing lists of classifiers generated by each fold. We'll need to score each classifier iteration for each again fold (i),
        # for each classifier (j) again when we come to score fresh data.
        fitted_clfs = {}

        # X_priv_scaled = scale_my_data(X_priv)
        # X_holdout_scaled = scale_my_data(X_holdout)

        # Loop over classifiers
        for j, (clf_name, stackable) in enumerate(clfs.items()):


            print(j, 'MODEL: ', clf_name, ':', '\n')
            if follow: print(clf, '\n')

            clf = stackable.clf
            features = stackable.features
            # target = stackable.target # should implement a check that model target matches stacker target
            algorithm = stackable.algorithm

            # Private & holdout data sets for selected features
            X_priv_clf = X_priv.loc[:,features]
            X_holdout_clf = X_holdout.loc[:,features]

            # Instantiate ROC tracking object
            ROC_tracker = roc.roc_cv(clf_name, n_folds = len(skf))

            # Created a reduced SHAP sample to pass in for performance tracking
            SHAP_idx = X_priv_clf.sample(n=SHAP_sample_size, replace=False).index
            X_SHAP = X_priv_clf.loc[SHAP_idx]

            if algorithm == 'lgbm':
                # Initialise object to track SHAP values
                clf_fi = fi.tree_shap_cv(name=clf_name, X=X_SHAP, n_folds = len(skf))
            elif algorithm == 'logr':
                # Initialise object to track logr coeffs
                clf_fi = fi.logr_coeffs_cv(name=clf_name, colnames=X_priv_clf.columns, n_folds = len(skf))

            # Instantiate a numpy array for the predictions on the private & holdout datasets, 
            # based on classifier j, generated by each fold i of the public dataset X.
            # rows: idx 
            # columns: fold i that generated model used for prediction
            # values: predictions from model i
            test_blend_j = np.zeros((X_priv.shape[0], len(skf)))
            ho_blend_j = np.zeros((X_holdout.shape[0], len(skf)))

            # Instantiate a list to store classifiers generated by each fold for scoring later
            fold_scalers = []
            fold_fitted_clfs = []

            # Loop through the folds, build a model on each fold of the public dataset X
            # with classifier j, keep predictions on test fold in the train predictions
            # set train_blend and add a column of predictions on the private dataset
            # to the numpy array above
            for i, (train_idx, test_idx) in skf:

                # Preparing train/test datasets for this fold
                if follow: print("Fold", i)
                X_train = X.iloc[train_idx].loc[:,features]
                y_train = y.iloc[train_idx].loc[:,target].values
                X_test = X.iloc[test_idx].loc[:,features]
                y_test = y.iloc[test_idx].loc[:,target].values

                # Application of scaling
                scaler = utils.tidy_scaler()
                X_train = scaler.fit_transform(X_train)
                X_test = scaler.transform(X_test)
                X_priv_ScaledForFold = scaler.transform(X_priv_clf)
                X_holdout_ScaledForFold = scaler.transform(X_holdout_clf)

                if follow: print("Scaling Data...")
                if follow: print(i,'= Train:', X_train.shape, 'Test:', X_test.shape)
                if follow: print("")

                # Fitting the classifiers
                if follow: print('Fitting classifier to public data training fold...')
                if algorithm == 'logr':
                    clf.fit(X_train, y_train)
                elif algorithm == 'lgbm':
                    clf.fit(X_train, y_train,
                            eval_set=[(X_test, y_test)],
                           early_stopping_rounds = 10,
                           verbose=False)
                elif algorithm == 'mlpnn':
                    clf.fit(X_train, y_train)
                if follow: print('Done\n')

                # Creating predictions on public test fold
                if follow: print('Predicting on public test fold...')
                fold_test_preds = clf.predict_proba(X_test)[:,1]
                if follow: print('Done\n')

                # Add this folds test predictions on public data X to train_blend
                train_blend[test_idx, j] = fold_test_preds

                # Create a vector of predictions with this classifier 
                # from this fold onto the private dataset
                private_preds = clf.predict_proba(X_priv_ScaledForFold)[:,1]
                holdout_preds = clf.predict_proba(X_holdout_ScaledForFold)[:,1]
                test_blend_j[:,i] = private_preds
                ho_blend_j[:,i] = holdout_preds

                # Add the fitted classifier for this fold to the list for this model
                fold_scalers.append(scaler)
                fold_fitted_clfs.append(clf)

                if follow: print('Fold',i,'completed\n\n')

                # Update the ROC tracker with a curve for this fold
                ROC_tracker.add_fold(y_test, fold_test_preds)

                # ML intepretation ingredients:
                ## logistic regressions
                if algorithm == 'logr':
                    # "Feature importance" (AKA coefficients)
                    clf_fi.add_coeffs(clf)
                elif algorithm == 'lgbm':
                    clf_fi.fit_shapley_fold(clf)

                # Print the log-loss for this classifier fold
                print('Fold '+str(i)+' log-loss: '+str(log_loss(y_test, fold_test_preds)))

            # Update the feature importance dictionary with feature importance data for this classifier
            fi_dict[clf_name] = clf_fi
            
            # Update the ROC dictionary with the ROC tracker for this classifier
            roc_dict[clf_name] = ROC_tracker

            # Update the fitted classifiers dictionary with the list of classifiers for this model
            stackable.fold_scalers = fold_scalers
            stackable.fold_fitted_clfs = fold_fitted_clfs

            # Store the feature importance object (which includes shap explainers)
            stackable.fi_builder = clf_fi

            # Add stackable to the list of fitted classifiers ('weak learners')
            fitted_clfs[clf_name] = stackable
              
            private_mean_preds = test_blend_j.mean(1)
            holdout_mean_preds = ho_blend_j.mean(1)
            test_blend[:,j] = private_mean_preds
            holdout_blend[:,j] = holdout_mean_preds

            # CONFUSION MATRIX:
            # Rather than generating a matrix for each fold (which would be basically
            # impossible to interpret), using the combined CV out-of-fold predictions
            # for all of the folds against private y. As none of the y predictions were
            # trained on the same fold as true y, we should be safe from information leak.
            # Using the class balance as a threshold by default.
            threshold = ROC_tracker.mean_optimal_threshold
            weak_learner_thresholds[clf_name] = threshold
            print("")
            print("Optimal threshold: "+str(ROC_tracker.mean_optimal_threshold))
            if plot_probability_dists:
                plt.hist(train_blend[:,j])
                plt.title(clf_name+" - predicted probability distribution")
                plt.show()
                print("")
            cm_dict[clf_name] = tprfpr.my_confusion_matrix(train_blend[:,j], y, threshold)

            print('\n')
        
        print("=======")

        self.weak_learners = fitted_clfs
        self.weak_learner_thresholds = weak_learner_thresholds

        print("Blending.\n")

        # Convert train_blend / test_blend / holdout_blend to pandas data frames and scale
        train_blend = pd.DataFrame(data = train_blend,
                                  columns = clf_names)
        test_blend = pd.DataFrame(data = test_blend,
                                 columns = clf_names)
        holdout_blend = pd.DataFrame(data = holdout_blend,
                                 columns = clf_names)

        stack_scaler = utils.tidy_scaler()
        train_blend = stack_scaler.fit_transform(train_blend)
        test_blend = stack_scaler.transform(test_blend)
        holdout_blend = stack_scaler.transform(holdout_blend)

        # Fit and predict the model blender
        clf, clf_name = LogisticRegression(), 'Stacker'
        clf.fit(train_blend, y.loc[:,target].values)
        y_predictions = clf.predict_proba(test_blend)[:,1]
        holdout_predictions = clf.predict_proba(holdout_blend)[:,1]

        # Print the log-loss against the private dataset
        self.priv_logl = avg_logl = round(log_loss(y_priv, y_predictions),2)
        print('Blended log-loss against private dataset: '+str(log_loss(y_priv, y_predictions)))

        # Print the log-loss against the out-of-time holdout
        self.holdout_logl = avg_logl = round(log_loss(y_holdout, holdout_predictions),2)
        print('Blended log-loss against out-of-time holdout: '+str(log_loss(y_holdout, holdout_predictions)))

        print()

        # Co-efficients with 1 fold:
        blend_fi = fi.logr_coeffs_cv(name=clf_name, colnames=test_blend.columns, n_folds = 1)
        blend_fi.add_coeffs(clf)

        # ROC tracker for the model blender with 1 fold
        blend_ROC = roc.roc_cv('Stacker', n_folds = 2)
        blend_ROC.add_fold(y_priv, y_predictions, label = 'Test ROC')
        blend_ROC.add_fold(y_holdout, holdout_predictions, label = 'Out-of-time ROC')

        print('Optimal Threshold: '+str(blend_ROC.mean_optimal_threshold))

        cm = tprfpr.my_confusion_matrix(y_predictions, y_priv, blend_ROC.mean_optimal_threshold)
        acc_score = tprfpr.accuracy_score(cm)
        f1_score = tprfpr.f1_score(cm)
        print('Accuracy Score: '+str(acc_score))
        print('F1 Score: '+str(f1_score))

        # Meta-learner properties
        self.stacker = clf
        self.stack_scaler = stack_scaler
        self.priv_predictions = y_predictions
        self.holdout_predictions = holdout_predictions
        self.roc = blend_ROC
        self.coeff_values = blend_fi
        self.confusion_matrix = cm
        self.scores = {
            'accuracy': acc_score,
            'f1': f1_score
        }

        # Dictionaries of weak learners
        self.fi_dict = fi_dict # Dictionary of feature importance objects for each weak learner
        self.roc_dict = roc_dict # Dictionary of 
        self.cm_dict = cm_dict

        self.fitted = True



    def predict(self, X, save_weak_learner_predictions=False, save_shap_sample=False, SHAP_sample_size=1000, follow=False):
        """
        Creating predictions for each weak learner, and the meta-learner, on fresh data.

        --- Parameters ---
        X: DataFrame
            Features set to score the stack against
        save_weak_learner_predictions: bool
            Optionally store a dataframe of the weak learner predictions to the stack
        save_shap_sample: bool
            Optionally store shap values AND their corresponding feature values to the stack
            for each classifier in the stack, for a sampled number of rows determined by
            SHAP_sample_size
        follow: bool
            Verbose logging
        """
        weak_learners = self.weak_learners
        weak_learner_thresholds = self.weak_learner_thresholds
        clf_names = weak_learners.keys()
        target = self.target

        # Dictionary to store feature importances
        fi_dict = {}

        # Set up matrices to store predicted probabilities and classes based on thresholds
        X_blend = np.zeros((X.shape[0], len(weak_learners)))
        if save_weak_learner_predictions:
            X_blend_classified = np.zeros((X.shape[0], len(weak_learners)))
        
        # If creating SHAP values, downsample the feature matrix and create a dictionary to store feature / shap array pairs
        if save_shap_sample:
            SHAP_idx = X.sample(n=min(SHAP_sample_size, X.shape[0]), replace=False).index
            SHAP_dict = {}

        # Remember the index for converting back to dataframes later
        X_index = X.index
        index_vars = X.index.names
        if index_vars == [None]:
            index_vars = ['index']

        # Loop over classifiers
        for j, (clf_name, stackable) in enumerate(weak_learners.items()):

            print(j, 'MODEL: ', clf_name, ':', '\n')
            if follow: print(clf, '\n')

            features = stackable.features
            algorithm = stackable.algorithm
            fi_builder = stackable.fi_builder

            n_fold = len(stackable.fold_fitted_clfs)

            # Prediction sets for selected features
            X_clf = X.loc[:,features]
            # Instantiate a numpy array for the predictions on the private & holdout datasets, 
            # based on classifier j, generated by each fold i of the public dataset X.
            # rows: idx 
            # columns: fold i that generated model used for prediction
            # values: predictions from model i
            weak_blend_j = np.zeros((X.shape[0], n_fold))

            # Loop through the folds, score each fold's corresponding fitted classifier on the
            # prediction features, and add the scores to the blending array
            for i in range(0, n_fold):

                # Preparing train/test datasets for this fold
                if follow: print("Fold", i)

                scaler = stackable.fold_scalers[i]
                clf = stackable.fold_fitted_clfs[i]

                # Application of scaling
                if follow: print("Scaling Data...")
                X_scaled = scaler.transform(X_clf)
                if follow: print("")

                # Create a vector of predictions with this classifier 
                # from this fold onto the prediction dataset
                weak_preds = clf.predict_proba(X_scaled)[:,1]
                weak_blend_j[:,i] = weak_preds
                if follow: print('Fold',i,'completed\n\n') 
              
            weak_mean_preds = weak_blend_j.mean(1)
            X_blend[:,j] = weak_mean_preds

            fi_dict[clf_name] = fi_builder.importances

            # Calculating binary classifications based on optimal thresholds for each weak learner
            if save_weak_learner_predictions:
                weak_classifications = weak_mean_preds.copy()
                weak_classifications[weak_classifications >= weak_learner_thresholds[clf_name]] = 1
                weak_classifications[weak_classifications < weak_learner_thresholds[clf_name]] = 0
                weak_classifications = weak_classifications.astype(int)
                X_blend_classified[:,j] = weak_classifications

            # Add the SHAP values and corresponding feature values to the SHAP dictionary
            # Rebuilding as pandas dataframes because we are tidy people - and it helps a lot with organising data downstream
            if save_shap_sample:

                df_X = pd.DataFrame(data = X_clf,
                                 index = X_index,
                                 columns = features)
                df_X = df_X.loc[SHAP_idx]
                df_SHAP = pd.DataFrame(
                    data = fi_builder.predict_shapley_folds(df_X),
                    index = df_X.index,
                    columns = features
                )

                ## Melting shap tables and joining
                df_X = pd.melt(df_X.reset_index(), id_vars=index_vars, var_name  = 'feature', value_name = 'input_value').set_index(index_vars+['feature'])
                df_SHAP = pd.melt(df_SHAP.reset_index(), id_vars=index_vars, var_name  = 'feature', value_name = 'shap_value').set_index(index_vars+['feature'])

                SHAP_dict[clf_name] = df_X.merge(df_SHAP, how='inner', left_index = True, right_index = True)
                # SHAP_dict[clf_name] = (df_X, df_SHAP)

            print('\n')
        
        print("=======")
        print("Stacking.\n")

        # Convert train_blend / test_blend / holdout_blend to pandas data frames and scale
        X_blend = pd.DataFrame(data = X_blend,
                               index = X_index,
                               columns = clf_names)
        if save_weak_learner_predictions:
            X_blend_classified = pd.DataFrame(data = X_blend_classified,
                                              index = X_index,
                                              columns = clf_names)

        # Fit and predict the model blender
        stack_scaler, clf, clf_name = self.stack_scaler, self.stacker, 'Stacker'
        X_blend = stack_scaler.transform(X_blend)
        y_predictions = clf.predict_proba(X_blend)[:,1]

        if save_weak_learner_predictions:
            self.weak_learner_predictions = X_blend
            self.weak_learner_classifications = X_blend_classified

        if save_shap_sample:
            self.shap_sample_dict = SHAP_dict

        print('Complete!\n')

        return y_predictions



    def weak_learner_coeffs(self):
        """
        Prints a plot of coefficients for each weak learner in the coalition
        """
        if self.fitted == False:
            return "Stacked model not yet fitted"

        plt.figure(figsize=(8,6))
        coeffs = abs(self.clf.coef_[0])
        featimp = 100.0 * (coeffs / coeffs.max())
        fi.my_featimp(featimp, self.clf_names, 'Stacked', 'logr')



    def stacked_roc(self):
        """
        Prints ROC plot for the stacked learner
        """
        plt.figure(figsize=(8,6))
        self.roc.roc_plot()

    

