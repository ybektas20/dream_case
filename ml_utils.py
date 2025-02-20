import numpy as np
import pandas as pd
from google.cloud import bigquery as bq
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, PowerTransformer, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegressionCV
from sklearn.metrics import classification_report, roc_auc_score

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

def get_X_y(df_metrics: pd.DataFrame) -> pd.DataFrame:
    df = df_metrics.copy()
    # Avoid division-by-zero issues:
    df['level_start'] = df['level_start'].replace(0, np.nan)
    df['time_spend'] = df['time_spend'].replace(0, np.nan)
    df['coin_spend'] = df['coin_spend'].replace(0, np.nan)

    # Engagement and Performance Metrics
    df['avg_time_per_level'] = df['time_spend'] / df['level_start']
    df['success_rate'] = df['level_success'] / df['level_start']
    df['failure_rate'] = df['level_fail'] / df['level_start']
    df['net_success'] = df['level_success'] - df['level_fail']

    # Spending and Earning Patterns
    df['net_coin'] = df['coin_earn'] - df['coin_spend']
    df['net_booster'] = df['booster_earn'] - df['booster_spend']
    df['coin_spend_rate'] = df['coin_spend'] / df['time_spend']
    df['booster_spend_rate'] = df['booster_spend'] / df['time_spend']
    df['coin_earn_rate'] = df['coin_earn'] / df['time_spend']
    df['booster_earn_rate'] = df['booster_earn'] / df['time_spend']
    df['booster_coin_ratio'] = df['booster_spend'] / df['coin_spend']


    # Additional Behavioral Metrics
    df['shop_frequency'] = df['shop_open'] / df['time_spend']

    # Fill any NaN values (due to division by zero) with 0.
    df.fillna(0, inplace=True)
    feature_cols = [
        'avg_time_per_level',
        'success_rate',
        'failure_rate',
        'net_success',
        'net_coin',
        'net_booster',
        'coin_spend_rate',
        'booster_spend_rate',
        'coin_earn_rate',
        'booster_earn_rate',
        'booster_coin_ratio',
        'shop_frequency',
        'age', 'time_spend',
        'coin_spend', 'coin_earn', 'level_success', 'level_fail', 'level_start',
        'booster_spend', 'booster_earn', 'coin_amount', 'event_participate',
        'shop_open',
        'country', 'platform', 'network'
    ]
    X = df[feature_cols]
    y = df['made_purchase']
    return X, y

