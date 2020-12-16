import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc, log_loss
from scipy import interp

def _roc_cv__iter_curve(fpr, tpr, auc, iter, label=None):

    if label==None:
        label='ROC fold %s' % (iter)
    auc_lab = ' (AUC = %0.2f)' % (auc)
    curve = plt.plot(
        fpr, tpr, lw=1, alpha=.3, label = label + auc_lab
    );
    return curve

class roc_cv(object):
    """
    Define an object for tracking ROC performance across folds of a
    cross-validated binary classifier. Collating true/false positive
    rates with methods for printing ROC curves with AUC and log-loss.
    """

    def __init__(self, name, n_folds=1):
        self.name = name
        self.n_folds = n_folds
        self.fold = 0

        # Instantiate lists for ROC curve ingredients
        self.curv_fprs = []
        self.curv_tprs = []
        self.labels = []

        # Instantiate lists for performance tracking
        self.tprs, self.aucs, self.mean_fpr = ([], [], np.linspace(0, 1, 100))
        curves = []
        self.logl = np.zeros((n_folds))
        self.optimal_thresholds = []

    def add_fold(self, y_true, y_score, label=None):

        fpr, tpr, thresholds = roc_curve(y_true, y_score)

        tprfpr = pd.DataFrame({
            'tpr':tpr,
            'fpr':fpr,
            'threshold':thresholds
        })
        tprfpr['diff'] = tprfpr.tpr - tprfpr.fpr
        tprfpr.sort_values(by=['diff'], ascending=False, inplace=True)
        self.optimal_thresholds.append(tprfpr.reset_index().threshold[0])
        self.mean_optimal_threshold = np.mean(self.optimal_thresholds)
        # self.tprfpr = tprfpr

        # Add log-loss for this fold
        self.logl[self.fold] = log_loss(y_true, y_score)

        # In the first fold, instantiate array for building curves.
        self.curv_fprs.append(fpr)
        self.curv_tprs.append(tpr)

        # Update true positive values
        self.tprs.append(interp(self.mean_fpr, fpr, tpr))

        # Update AUC value
        self.aucs.append(auc(fpr, tpr))

        # Add label to list
        self.labels.append(label)

        self.fold += 1


    def roc_plot(self):

        curv_fprs = self.curv_fprs
        curv_tprs = self.curv_tprs
        tprs = self.tprs
        aucs = self.aucs

        [__iter_curve(curv_fprs[i], curv_tprs[i], aucs[i], i, self.labels[i]) for i in range(0,self.n_folds)]
    
        plt.plot([0,1], [0,1], linestyle='--', lw=2,
             color='r', label='Chance', alpha=.8)
    
        mean_tpr = np.mean(self.tprs, axis=0)
        mean_tpr[-1] = 1.0
        mean_auc = auc(self.mean_fpr, mean_tpr)
        std_auc = np.std(self.aucs)

        # Calculate optimal threshold
        print("head mean_tpr: "+str(mean_tpr[0:9]))
        print("head mean_fpr: "+str(self.mean_fpr[0:9]))


        # Calculate average log-loss
        avg_logl = str(round(self.logl.mean(0),3))

        plt.plot(self.mean_fpr, mean_tpr, color='b',
            label=r'Mean ROC (AUC = %0.2f $\pm$ %0.2f)' % (mean_auc, std_auc),
            lw=2, alpha=.8)
    
    
        std_tpr = np.std(self.tprs, axis=0)
        tprs_upper = np.minimum(mean_tpr + std_tpr, 1)
        tprs_lower = np.maximum(mean_tpr - std_tpr, 0)
        plt.fill_between(self.mean_fpr, tprs_lower, tprs_upper, color='grey', alpha=.2,
                   label = r'$\pm$ 1 std. dev')
    
        plt.xlim([-0.05, 1.05])
        plt.ylim([-0.05, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('Reciever Operator Characteristic - '+self.name+' - Avg. Log-loss: '+avg_logl)
        plt.legend(loc='lower right')

