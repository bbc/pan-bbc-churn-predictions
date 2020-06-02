# SHAP model explainers

import shap
import numpy as np
import pandas as pd
import gzip
import csv
import matplotlib.pyplot as plt
from shap.plots import colors
# from src import stack_shap

def my_featimp(importance, X_cols, clf_name, clf_model, n_show=20):
    
    # Ordering by importance & creating axis vals
    n_show = min(len(importance), n_show)
    idx = np.argsort(importance)[-n_show::1]
    #idx = idx[:min(len(importance), n_show)]
    pos = np.arange(idx.shape[0]) + .5
    
    # Create plot
    plt.barh(pos, importance[idx], align='center')
    plt.yticks(ticks = pos, labels = np.array(X_cols)[idx], fontsize = 11)
    plt.xlabel('Feature Importance')
    plt.title('Feature Importance - '+clf_name)
    
    return plt.show()

class tree_shap_cv(object):
    """
    Define a SHAP object we can use for TreeSHAP feature importance.
    Tracking the useful ingredients of SHAP feature importance on a model, 
    with plots implemented as methods.

    ---  Parameters ---
    X: pandas dataframe
        Private dataset on which to visualise feature and SHAP value interactions.
    clf: sklearn classifier
        Fitted tree-based classifier model from the sklearn API.
    n_folds: integer
        Number of cross-validation folds to be performed.
    
    -- Attributes --
    explainer: shap.TreeExplainer
        tree explainer, built with the SHAP package from Lundberg & Lee
    shap_values_array: 3D array
        fitted shapley values from the tree explainer [rows, features, folds]
    SHAP_X_sample: 
        (sampled) dataset from the parent array
    SHAP_idx: 
        indices of the sampled rows used for SHAP in the parent array
    expected_values:
        the "base value" for each model created by each CV fold.

    -- Methods --
    fit_shapley_fold: 
        fit the shapley values of for this model's explainer on a dataset.
        fitted rows are sampled to reduce the size of class object - we
        don't need to explain every row in the whole dataset
    predicted_shapley_folds:
        create shapley values on a fresh dataset using explainers from the folds
        added to this model by `fit_shapley_fold`. Stores the shapley values as a
        column for each fold's explainer in a matrix, returns the matrix.
    random_force_plot:
        plot a force plot on a randomly selected row from the SHAP sample.
        non-deterministic.
    global_force_plot:
        force plots for all the rows in  the sample dataset, transposed,
        placed side-by-side and ordered by similarity to visualise patterns
        in features on SHAP impacts.
    summary_plot:
        violin plots for each feature, showing the distribution of SHAP value
        for each feature. Also includes the color bar to differentiate high
        and low feature values across the SHAP distributions.
    feature_dependence_plot:
        for a specific feature, a scatterplot of SHAP values against feature values.
        Very seful for visualising relationships between a feature and it's contribution
        to the model prediction.
    fi_plot:
        feature importance plot by feature. Here feature importance is defined
        as the mean value (across data points) of the absolute mean shapley values (across folds).
    decision_plot:
        for a small sample of test data points, starting from the models base prediction value,
        incrementely adding features to each prediction and plotting the path of the model score
        as a continuous line. Good for visualising how features across the model combine to create
        a diverse range of model scores.
    """

    def __init__(self, name, X, n_folds=1, sample_size = 1000):
        """
        Initialise a TreeSHAP analysis process with a dataset (X), sampled down to a given sample size
        """
        self.is_fitted = False
        self.n_folds = n_folds
        self.fold = 0
        self.model_type = 'gbm'
        self.name = name

        sample_size = min(sample_size, X.shape[0])
        sample_idx = X.sample(n = sample_size, replace = False).index
        X_sample = X.loc[sample_idx]

        self.shapley_values_array = np.zeros((X_sample.shape[0], X_sample.shape[1], self.n_folds))
        self.fold_expected_values = [0] * n_folds

        self.SHAP_idx = sample_idx
        self.SHAP_X_sample = X_sample
        self.explainers = []
        self.importances = pd.DataFrame(data=np.zeros((X_sample.shape[1], n_folds)),
                                        index = X.columns,
                                        columns = range(0, n_folds))
        # self.feature_names = X.columns

    def fit_shapley_fold(self, clf):
        """
        Fit the shapley values of for this model's explainer on a dataset.
        fitted rows are sampled to reduce the size of class object - we
        don't need to explain every row in the whole dataset
        * Build an explainer for a given classifier on this TreeSHAP iteration.
        * Create an array of shapley values for this fold, and add it to the parent array
          (shapley_values_array)
        """
        explainer = shap.TreeExplainer(clf)
        self.explainers.append(explainer)

        shapley_values = explainer.shap_values(self.SHAP_X_sample)[0]
        self.shapley_values_array[:,:,self.fold] = shapley_values

        self.fold_expected_values[self.fold] = explainer.expected_value[0]

        # Update importance array
        self.importances.iloc[:,self.fold] = clf.feature_importances_
        
        self.fold += 1
        self.fitted = True


    def predict_shapley_folds(self, X):
        """
        Create shapley values on a fresh dataset using explainers from the folds
        added to this model by `fit_shapley_fold`. Stores the shapley values as a
        column for each fold's explainer in a matrix, returns the matrix.
        """
        shapley_array = np.zeros((X.shape[0], X.shape[1], len(self.explainers)))

        for (i, explainer) in enumerate(self.explainers):
            shapley_array[:,:,i] = explainer.shap_values(X)[0]

        shap_values = shapley_array.mean(2)

        return(shap_values)


    # VISUALISATION FUNCTIONS =============

    def summary_plot(self, title=None, cmap=None):
        """
        Violin plots for each feature, showing the distribution of SHAP value
        for each feature. Also includes the color bar to differentiate high
        and low feature values across the SHAP distributions.
        """
        shap_values = self.shapley_values_array.mean(2)
        X = self.SHAP_X_sample

        return shap.summary_plot(shap_values, X, title = title, color=cmap)



    def fi_plot(self):
        """
        Feature importance plot by feature. Here feature importance is defined
        as the mean value (across data points) of the absolute mean shapley values (across folds).
        """
        shap_values = self.shapley_values_array.mean(2)
        X = self.SHAP_X_sample

        return shap.summary_plot(shap_values, X, plot_type = 'bar')



    def feature_dependence_plot(self, feature_name, cmap=None, color=None, interaction="auto", title=None, ax=None, dot_size=16, show=False):
        """
        For a specific feature, a scatterplot of SHAP values against feature values.
        Very seful for visualising relationships between a feature and it's contribution
        to the model prediction.
        """
        shap_values = self.shapley_values_array.mean(2)
        X = self.SHAP_X_sample

        return shap.dependence_plot(feature_name, shap_values, X, interaction_index=interaction,
                                    cmap=cmap, color=color, alpha=.6, x_jitter=1, dot_size = dot_size,
                                    title=title, ax=ax, show=show)



    def random_force_plot(self, fold, seed=None, outfile=None):
        """
        Plot a force plot on a randomly selected row from the SHAP sample.
        non-deterministic.
        """
        np.random.seed(seed)
        expected_value = self.fold_expected_values[fold]
        shap_values = self.shapley_values_array[:,:,fold]
        X = self.SHAP_X_sample

        X_len = X.shape[0]
        idx = np.random.choice(X_len, replace=False, size=1)[0]

        if outfile is not None:
            shap.force_plot(expected_value, shap_values[idx,:], X.iloc[idx,:], show=False, matplotlib=True).savefig(outfile)

        return shap.force_plot(expected_value, shap_values[idx,:], X.iloc[idx,:])



    def global_force_plot(self, fold, maxplotsize=100):
        """
        Force plots for all the rows in  the sample dataset, transposed,
        placed side-by-side and ordered by similarity to visualise patterns
        in features on SHAP impacts.
        """
        X = self.SHAP_X_sample

        maxsize = min(X.shape[0], maxplotsize)
        expected_value = self.fold_expected_values[fold]
        shap_values = self.shapley_values_array[:,:,fold]
        

        return shap.force_plot(expected_value, shap_values[:maxsize,:], X.iloc[:maxsize,:])



    def decision_plot(self, fold, supersample_frac=.1, feature_display_range=None, cmap=None):
        """
        For a small sample of test data points, starting from the models base prediction value,
        incrementely adding features to each prediction and plotting the path of the model score
        as a continuous line. Good for visualising how features across the model combine to create
        a diverse range of model scores.
        """
        expected_value = self.fold_expected_values[fold]
        shap_values = self.shapley_values_array[:,:,fold]
        X = self.SHAP_X_sample

        super_sample = X.sample(frac = supersample_frac, replace = False)
        super_sample_idx = super_sample.reset_index().index.values

        super_sampled_shapley_values = shap_values[super_sample_idx,:]

        return shap.decision_plot(
            expected_value,
            super_sampled_shapley_values,
            super_sample,
            feature_display_range=feature_display_range,
            plot_color = cmap
        )


class logr_coeffs_cv(object):
    """
    Define an object to track the coefficients across folds of a
    cross-validated logistic regression model.

    ---  Parameters ---
    clf: sklearn classifier
        Fitted tree-based classifier model from the sklearn API.
    n_folds: integer
        Number of cross-validation folds to be performed.
    
    -- Attributes --


    -- Methods --

    """
    def __init__(self, name, colnames, n_folds = 1):
        self.is_fitted = False
        self.n_folds = n_folds
        self.fold = 0
        self.model_type = 'logr'
        self.name = name

        self.coeffs_array = np.zeros((len(colnames), self.n_folds))
        self.colnames = colnames

    def add_coeffs(self, clf):
        """
        Add coefficients for a new fold of the cross-validated logr model.
        """
        coeffs = clf.coef_[0]
        self.coeffs_array[:,self.fold] = coeffs / abs(coeffs).max()
        self.importances = self.coeffs_array.mean(axis=1)
 
        self.fold += 1

    def fi_plot(self, n_show=20, title=None):

        importance = abs(self.coeffs_array).mean(1)
    
        # Ordering by importance & creating axis vals
        n_show = min(len(importance), n_show)
        idx = np.argsort(importance)[-n_show::1]
        #idx = idx[:min(len(importance), n_show)]
        pos = np.arange(idx.shape[0]) + .5
    
        # Create plot
        plt.barh(pos, importance[idx], align='center')
        plt.yticks(ticks = pos, labels = np.array(self.colnames)[idx], fontsize = 11)
        plt.xlabel('|Coefficient|')
        plt.title('Feature Importance -'+self.name)

    def coeff_plot(self, n_show=20, title=None):

        coeffs = self.coeffs_array.mean(1)
        pos_neg = coeffs > 0

        shap_blu = colors.blue_rgb
        shap_red = colors.red_rgb

        # Ordering by importance & creating axis vals
        n_show = min(len(coeffs), n_show)
        idx = np.argsort(abs(coeffs))[-n_show::1]
        #idx = idx[:min(len(importance), n_show)]
        pos = np.arange(idx.shape[0]) + .5

        y = coeffs[idx]

        # Create plot
        plt.barh(pos[y >= 0], y[y >= 0], align='center', color = shap_red)
        plt.barh(pos[y < 0], y[y < 0], align='center', color = shap_blu)
        plt.yticks(ticks = pos, labels = np.array(self.colnames)[idx], fontsize = 11)
        plt.xlabel('Average Regression Coefficient')
        plt.title('Coefficient Plot -'+self.name)