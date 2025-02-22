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
    g.map(sns.histplot, "value", bins=bins, kde = True)
    
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

def plot_scatter_with_labels(X, y, feature_x, feature_y, min_quantile=0.0, sampling_raito = 0.2):
    """
    Creates a scatter plot for two features with points colored by their target label.
    
    Parameters:
    - X: pandas DataFrame containing the feature data.
    - y: pandas Series or array-like containing the target labels.
    - feature_x: str, the name of the feature to plot on the x-axis.
    - feature_y: str, the name of the feature to plot on the y-axis.
    - min_quantile: float, quantile threshold to filter out extreme values (default is 0.0, i.e., no filtering).
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # Create a copy of the data and add the target label.
    data = X.copy()
    data['target'] = y

    data = data.sample(frac=sampling_raito, random_state=42)

    try:
        # Compute quantile limits for both features.
        min_limit_x = data[feature_x].quantile(min_quantile)
        max_limit_x = data[feature_x].quantile(1 - min_quantile)
        min_limit_y = data[feature_y].quantile(min_quantile)
        max_limit_y = data[feature_y].quantile(1 - min_quantile)
        
        # Filter out extreme values.
        data_filtered = data[
            (data[feature_x] > min_limit_x) & (data[feature_x] < max_limit_x) &
            (data[feature_y] > min_limit_y) & (data[feature_y] < max_limit_y)
        ]
        
        # Determine unique target values.
        unique_targets = sorted(data_filtered['target'].unique())
        
        # Create a blue-red palette that is not too shiny.
        if len(unique_targets) == 2:
            # For binary targets, use fixed muted colors.
            palette = {unique_targets[0]: "#4c72b0",  # Muted blue
                       unique_targets[1]: "#c44e52"}  # Muted red
        else:
            # For multiple classes, generate a diverging blue-to-red palette.
            palette = sns.color_palette("coolwarm", n_colors=len(unique_targets))
        
        # Create the scatter plot.
        plt.figure(figsize=(10, 5))
        sns.scatterplot(data=data_filtered, x=feature_x, y=feature_y, hue='target', palette=palette)
        plt.title(f"Scatter Plot of {feature_x} vs {feature_y} Colored by Target")
        plt.xlabel(feature_x)
        plt.ylabel(feature_y)
        plt.show()
        
    except Exception as e:
        print(f"Could not plot scatter plot for features {feature_x} and {feature_y}.")
        print(e)