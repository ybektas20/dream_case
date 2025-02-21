import numpy as np
import pandas as pd
from google.cloud import bigquery as bq
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc, precision_recall_curve
import matplotlib.pyplot as plt
import seaborn as sns


def load_data(service_account_path: str) -> pd.DataFrame:
    """Load data from BigQuery and create target variable."""
    client = bq.Client.from_service_account_json(service_account_path)
    query = """
    SELECT *
    FROM `casedreamgames.case_db.q3_table_user_metrics`
    """
    df = client.query(query).result().to_dataframe()
    # Create binary target: made_purchase=1 if d30_revenue > 0, else 0.
    df['made_purchase'] = (df['d30_revenue'] > 0).astype(int)
    return df
"""
'user_id', 'country', 'age', 'platform', 'network', 'time_spend',
       'coin_spend', 'coin_earn', 'level_success', 'level_fail', 'level_start',
       'booster_spend', 'booster_earn', 'coin_amount', 'event_participate',
       'shop_open', 'd30_revenue', 'made_purchase'
"""
def get_X_y(df_metrics: pd.DataFrame) -> pd.DataFrame:
    df = df_metrics.copy()
    X = df[[ 'age', 'time_spend', 'level_success',
         'event_participate', 'coin_amount', 'booster_spend',
        'shop_open', 'country', 'platform', 'network']]
    
    # Engagement and Performance Metrics
    X['time_spend_rate'] = df['time_spend'] / (df['level_start']+1)
    X['success_rate'] = df['level_success'] / (df['level_start'] + 1)

    # Spending and Earning Patterns
    X['net_coin'] = df['coin_earn'] - df['coin_spend']
    X['net_booster'] = df['booster_earn'] - df['booster_spend']

    #X['net_coin_freq'] = (df['coin_earn'] - df['coin_spend'])/(df['time_spend'] + 1)
    X['net_coin_rate'] = (df['coin_earn'] - df['coin_spend'])/(df['level_start'] + 1)
    X['net_booster_freq'] = (df['booster_earn'] - df['booster_spend']) / (df['time_spend'] + 1)
    #X['net_booster_rate'] = (df['booster_earn'] - df['booster_spend']) / (df['level_start'] + 1)
    X['booster_spend_ratio'] = df['booster_spend'] / (df['booster_earn'] + df['booster_spend'] + 1)
    X['coin_spend_ratio'] = df['coin_spend'] / (df['coin_amount'] + 1)

    # Additional Behavioral Metrics
    #X['shop_open_rate'] = df['shop_open'] / (df['level_start'] + 1)
    X['shop_open_frequency'] = df['shop_open'] / (df['time_spend'] + 1)
    #X['event_participate_rate'] = df['event_participate'] / (df['level_start'] + 1)
    X['event_participate_frequency'] = df['event_participate'] / (df['time_spend'] + 1)

    y = df['made_purchase']
    return X, y


def plot_histograms(X_train, bins=50, col_wrap=5, height=3):
    """
    Plots histograms for each column in X_train (after dropping one-hot encoded columns)
    using Seaborn's FacetGrid.

    Parameters:
    - X_train: DataFrame containing the training data.
    - one_hot_encoded_columns: List of column names to drop.
    - bins: Number of bins for each histogram (default is 50).
    - col_wrap: Number of plots per row (default is 5).
    - height: Height of each subplot (default is 3).
    """
    # Drop one-hot encoded columns from the DataFrame
    df = X_train
    
    # Reshape the DataFrame from wide to long format
    df_melted = df.melt(var_name='variable', value_name='value')
    
    # Create a FacetGrid for the histograms
    g = sns.FacetGrid(df_melted, col="variable", col_wrap=col_wrap, sharex=False, sharey=False, height=height)
    g.map(sns.histplot, "value", bins=bins)
    
    # Adjust the overall title and layout
    g.fig.subplots_adjust(top=0.9)
    g.fig.suptitle('Histograms of Variables', fontsize=16)
    
    # Display the plots
    plt.show()


def plot_conditional_distributions(X, y, min_quantile=0.0):
    X_cond = X.copy()
    X_cond['target'] = y

    for col in X.columns:
        try:
            min_limit = X_cond[col].quantile(min_quantile)
            max_limit = X_cond[col].quantile(1-min_quantile)
            data = X_cond.loc[(X_cond[col]>min_limit) & (X_cond[col]<max_limit),[col, 'target']]
            plt.figure(figsize=(10, 5))
            sns.histplot(data=data, x=col, hue='target', element='step', stat='density', common_norm=False)
            plt.title(f"Conditional Distribution of {col} (by target)")
            plt.xlabel(col)
            plt.ylabel("Density")
            plt.show()
        except Exception as e:
            print(f"Could not plot {col}")
            print(e)

def plot_roc_auc(y_true, y_pred_prob):
    """
    Plots the ROC AUC curve.
    
    Parameters:
    y_true (array-like): True binary labels.
    y_pred_prob (array-like): Target scores, can either be probability estimates of the positive class, confidence values, or binary decisions.
    """
    fpr, tpr, _ = roc_curve(y_true, y_pred_prob)
    roc_auc = auc(fpr, tpr)
    
    plt.figure(figsize=(10, 6))
    plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (area = {roc_auc:.2f})')
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve')
    plt.legend(loc="lower right")
    plt.grid(True)
    plt.show()

def plot_precision_recall(y_true, y_pred_prob):
    """
    Plots the Precision-Recall curve.
    
    Parameters:
    y_true (array-like): True binary labels.
    y_pred_prob (array-like): Target scores, can either be probability estimates of the positive class, confidence values, or binary decisions.
    """
    precision, recall, _ = precision_recall_curve(y_true, y_pred_prob)
    
    plt.figure(figsize=(10, 6))
    plt.plot(recall, precision, color='blue', lw=2)
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Precision-Recall Curve')
    plt.grid(True)
    plt.show()

import matplotlib.pyplot as plt
from sklearn.calibration import calibration_curve

def plot_calibration_curve(y_true, y_prob, n_bins=10, title="Calibration Curve"):
    """
    Plots a calibration curve for binary classification.
    
    Parameters:
        y_true (array-like): True binary labels (0 or 1)
        y_prob (array-like): Predicted probabilities for the positive class
        n_bins (int): Number of bins to use for calibration
        title (str): Title of the plot
    
    Returns:
        None (Displays the calibration plot)
    """
    # Compute calibration curve
    prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=n_bins)
    
    # Create plot
    plt.figure(figsize=(8, 8))
    plt.plot(prob_pred, prob_true, marker='o', linewidth=2, label='Calibration curve')
    plt.plot([0, 1], [0, 1], linestyle='--', color='gray', label='Perfect calibration')
    plt.xlabel("Mean predicted probability")
    plt.ylabel("Fraction of positives")
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import roc_curve, auc
from sklearn.calibration import calibration_curve
from sklearn.metrics import roc_curve, auc, precision_recall_curve, average_precision_score

def plot_roc_curves(true_labels, proba_df, labels=None):
    """
    Plot ROC curves for multiple models.
    
    Parameters:
    - true_labels: array-like, the true binary labels.
    - proba_df: pandas DataFrame, each column contains predicted probabilities from a model.
    - labels: list of strings, optional, names for the models. If None, uses proba_df columns.
    """
    if labels is None:
        labels = proba_df.columns.tolist()
    
    plt.figure(figsize=(8, 6))
    # Plot the chance line.
    plt.plot([0, 1], [0, 1], linestyle='--', color='gray', label='Chance')
    
    # Compute and plot ROC curve for each model.
    for col, label in zip(proba_df.columns, labels):
        fpr, tpr, _ = roc_curve(true_labels, proba_df[col])
        roc_auc = auc(fpr, tpr)
        plt.plot(fpr, tpr, label=f'{label} (AUC = {roc_auc:.2f})')
    
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curves")
    plt.legend(loc='lower right')
    plt.grid(True)
    plt.show()


def plot_calibration_curves(true_labels, proba_df, n_bins=10, labels=None):
    """
    Plot calibration curves for multiple models.
    
    Parameters:
    - true_labels: array-like, the true binary labels.
    - proba_df: pandas DataFrame, each column contains predicted probabilities from a model.
    - n_bins: int, number of bins to use in calibration curve.
    - labels: list of strings, optional, names for the models. If None, uses proba_df columns.
    """
    if labels is None:
        labels = proba_df.columns.tolist()
    
    plt.figure(figsize=(8, 6))
    # Plot the perfectly calibrated line.
    plt.plot([0, 1], [0, 1], linestyle='--', color='gray', label='Perfectly Calibrated')
    
    # Compute and plot calibration curves.
    for col, label in zip(proba_df.columns, labels):
        prob_true, prob_pred = calibration_curve(true_labels, proba_df[col], n_bins=n_bins)
        plt.plot(prob_pred, prob_true, marker='o', label=label)
    
    plt.xlabel("Mean Predicted Probability")
    plt.ylabel("Fraction of Positives")
    plt.title("Calibration Curves")
    plt.legend(loc='best')
    plt.grid(True)
    plt.show()


def plot_precision_recall_curves(true_labels, proba_df, labels=None):
    """
    Plot Precision-Recall curves for multiple models.
    
    Parameters:
    - true_labels: array-like, the true binary labels.
    - proba_df: pandas DataFrame, each column contains predicted probabilities from a model.
    - labels: list of strings, optional, names for the models. If None, uses proba_df columns.
    """
    if labels is None:
        labels = proba_df.columns.tolist()
    
    plt.figure(figsize=(8, 6))
    
    # Compute and plot Precision-Recall curves.
    for col, label in zip(proba_df.columns, labels):
        precision, recall, _ = precision_recall_curve(true_labels, proba_df[col])
        avg_prec = average_precision_score(true_labels, proba_df[col])
        plt.plot(recall, precision, label=f'{label} (AP = {avg_prec:.2f})')
    
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title("Precision-Recall Curves")
    plt.legend(loc='lower left')
    plt.grid(True)
    plt.show()