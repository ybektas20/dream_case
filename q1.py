import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery as bq

# Initialize the BigQuery client using your service account JSON
SERVICE_ACCOUNT_PATH = '/home/yusuf/DataScience/dream_games/ybektas20.json'
client = bq.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)

# ============================
# Query Functions
# ============================

def get_total_dau():
    query = """
    SELECT DATE(event_time) AS date,
           COUNT(DISTINCT user_id) AS dau
    FROM `casedreamgames.case_db.q1_table_session`
    GROUP BY date
    ORDER BY date;
    """
    return client.query(query).result().to_dataframe()

def get_dau_by_platform():
    query = """
    SELECT DATE(event_time) AS date,
           platform,
           COUNT(DISTINCT user_id) AS dau
    FROM `casedreamgames.case_db.q1_table_session`
    GROUP BY date, platform
    ORDER BY platform, date;
    """
    return client.query(query).result().to_dataframe()

def get_overall_arpdau():
    query = """
    WITH daily_revenue AS (
      SELECT DATE(event_time) AS date, SUM(CAST(revenue AS FLOAT64)) AS total_revenue
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY date
    ),
    daily_active AS (
      SELECT DATE(event_time) AS date, COUNT(DISTINCT user_id) AS dau
      FROM `casedreamgames.case_db.q1_table_session`
      GROUP BY date
    )
    SELECT dr.date,
           dr.total_revenue / da.dau AS arpdau
    FROM daily_revenue dr
    JOIN daily_active da ON dr.date = da.date
    ORDER BY dr.date;
    """
    return client.query(query).result().to_dataframe()

def get_arpdau_by_platform():
    query = """
    WITH revenue_platform AS (
      SELECT DATE(event_time) AS date, platform, SUM(CAST(revenue AS FLOAT64)) AS total_revenue
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY date, platform
    ),
    dau_platform AS (
      SELECT DATE(event_time) AS date, platform, COUNT(DISTINCT user_id) AS dau
      FROM `casedreamgames.case_db.q1_table_session`
      GROUP BY date, platform
    )
    SELECT r.platform,
           r.date,
           r.total_revenue / d.dau AS arpdau
    FROM revenue_platform r
    JOIN dau_platform d ON r.date = d.date AND r.platform = d.platform
    ORDER BY r.platform, r.date;
    """
    return client.query(query).result().to_dataframe()

def get_arpdau_by_package_type():
    query = """
    WITH daily_package_revenue AS (
      SELECT package_type,
             DATE(event_time) AS date,
             SUM(CAST(revenue AS FLOAT64)) AS total_revenue,
             COUNT(DISTINCT user_id) AS purchasing_users
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY package_type, date
    )
    SELECT package_type,
           date,
           total_revenue / purchasing_users AS arpdau
    FROM daily_package_revenue
    ORDER BY package_type, date;
    """
    return client.query(query).result().to_dataframe()

def get_overall_retention():
    query = """
    WITH installs AS (
      SELECT user_id, DATE(event_time) AS install_date
      FROM `casedreamgames.case_db.q1_table_install`
    ),
    next_day_sessions AS (
      SELECT user_id, DATE(event_time) AS session_date
      FROM `casedreamgames.case_db.q1_table_session`
    )
    SELECT i.install_date,
           COUNT(DISTINCT i.user_id) AS installs,
           COUNT(DISTINCT n.user_id) AS retained_users,
           COUNT(DISTINCT n.user_id) / COUNT(DISTINCT i.user_id) AS retention_rate
    FROM installs i
    LEFT JOIN next_day_sessions n 
      ON i.user_id = n.user_id 
      AND n.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
    GROUP BY i.install_date
    ORDER BY i.install_date;
    """
    return client.query(query).result().to_dataframe()

def get_retention_by_platform():
    query = """
    WITH installs AS (
      SELECT user_id, platform, DATE(event_time) AS install_date
      FROM `casedreamgames.case_db.q1_table_install`
    ),
    next_day_sessions AS (
      SELECT user_id, DATE(event_time) AS session_date
      FROM `casedreamgames.case_db.q1_table_session`
    )
    SELECT i.platform,
           i.install_date,
           COUNT(DISTINCT i.user_id) AS installs,
           COUNT(DISTINCT n.user_id) AS retained_users,
           COUNT(DISTINCT n.user_id) / COUNT(DISTINCT i.user_id) AS retention_rate
    FROM installs i
    LEFT JOIN next_day_sessions n 
      ON i.user_id = n.user_id 
      AND n.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
    GROUP BY i.platform, i.install_date
    ORDER BY i.platform, i.install_date;
    """
    return client.query(query).result().to_dataframe()

def get_overall_roas():
    query = """
    WITH daily_revenue AS (
      SELECT DATE(event_time) AS date,
             SUM(CAST(revenue AS FLOAT64)) AS total_revenue
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY date
    ),
    daily_cost AS (
      SELECT date, SUM(cost) AS total_cost
      FROM `casedreamgames.case_db.q1_table_cost`
      GROUP BY date
    )
    SELECT r.date,
           r.total_revenue,
           c.total_cost,
           r.total_revenue / c.total_cost AS roas
    FROM daily_revenue r
    JOIN daily_cost c ON r.date = c.date
    ORDER BY r.date;
    """
    return client.query(query).result().to_dataframe()

def get_roas_by_platform():
    query = """
    WITH daily_revenue AS (
      SELECT DATE(event_time) AS date, platform,
             SUM(CAST(revenue AS FLOAT64)) AS total_revenue
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY date, platform
    ),
    daily_cost AS (
      SELECT date, SUM(cost) AS total_cost
      FROM `casedreamgames.case_db.q1_table_cost`
      GROUP BY date
    )
    SELECT r.platform,
           r.date,
           r.total_revenue,
           c.total_cost,
           r.total_revenue / c.total_cost AS roas
    FROM daily_revenue r
    JOIN daily_cost c ON r.date = c.date
    ORDER BY r.platform, r.date;
    """
    return client.query(query).result().to_dataframe()

def get_roas_by_package_type():
    query = """
    WITH daily_revenue AS (
      SELECT DATE(event_time) AS date, package_type,
             SUM(CAST(revenue AS FLOAT64)) AS total_revenue
      FROM `casedreamgames.case_db.q1_table_revenue`
      GROUP BY date, package_type
    ),
    daily_cost AS (
      SELECT date, SUM(cost) AS total_cost
      FROM `casedreamgames.case_db.q1_table_cost`
      GROUP BY date
    )
    SELECT r.package_type,
           r.date,
           r.total_revenue,
           c.total_cost,
           r.total_revenue / c.total_cost AS roas
    FROM daily_revenue r
    JOIN daily_cost c ON r.date = c.date
    ORDER BY r.package_type, r.date;
    """
    return client.query(query).result().to_dataframe()

# ============================
# Tab Functions (Modular)
# ============================

def user_engagement_tab():
    st.header("User Engagement")
    # Unique key for this selectbox
    grouper = st.selectbox("Select Grouper", options=["None", "Platform"], index=0, key="user_engagement_grouper")
    
    if grouper == "None":
        st.subheader("Total Daily Active Users (DAU)")
        df = get_total_dau()
        fig = px.line(df, x='date', y='dau', title="Total DAU Over Time")
        st.plotly_chart(fig)
    elif grouper == "Platform":
        st.subheader("DAU by Platform")
        df = get_dau_by_platform()
        fig = px.line(df, x='date', y='dau', color='platform', 
                      title="DAU by Platform Over Time")
        st.plotly_chart(fig)

def monetization_tab():
    st.header("Monetization Performance")
    # Unique key for this selectbox
    grouper = st.selectbox("Select Grouper", options=["None", "Platform", "Package Type"], index=0, key="monetization_grouper")
    
    if grouper == "None":
        st.subheader("Overall ARPDAU")
        df = get_overall_arpdau()
        fig = px.line(df, x='date', y='arpdau', title="Overall ARPDAU Over Time")
        st.plotly_chart(fig)
    elif grouper == "Platform":
        st.subheader("ARPDAU by Platform")
        df = get_arpdau_by_platform()
        fig = px.line(df, x='date', y='arpdau', color='platform', 
                      title="ARPDAU by Platform Over Time")
        st.plotly_chart(fig)
    elif grouper == "Package Type":
        st.subheader("ARPDAU by Package Type")
        df = get_arpdau_by_package_type()
        fig = px.line(df, x='date', y='arpdau', color='package_type', 
                      title="ARPDAU by Package Type Over Time")
        st.plotly_chart(fig)

def retention_tab():
    st.header("Retention")
    # Unique key for this selectbox
    grouper = st.selectbox("Select Grouper", options=["None", "Platform"], index=0, key="retention_grouper")
    
    if grouper == "None":
        st.subheader("Overall Next-Day Retention")
        df = get_overall_retention()
        fig = px.line(df, x='install_date', y='retention_rate', 
                      title="Overall Next-Day Retention Over Time")
        st.plotly_chart(fig)
    elif grouper == "Platform":
        st.subheader("Next-Day Retention by Platform")
        df = get_retention_by_platform()
        fig = px.line(df, x='install_date', y='retention_rate', color='platform',
                      title="Next-Day Retention by Platform Over Time")
        st.plotly_chart(fig)

def marketing_tab():
    st.header("Marketing Effectiveness & Acquisition")
    # Unique key for this selectbox
    grouper = st.selectbox("Select Grouper", options=["None", "Platform", "Package Type"], index=0, key="marketing_grouper")
    
    if grouper == "None":
        st.subheader("Overall ROAS")
        df = get_overall_roas()
        fig = px.line(df, x='date', y='roas', title="Overall ROAS Over Time")
        st.plotly_chart(fig)
    elif grouper == "Platform":
        st.subheader("ROAS by Platform")
        df = get_roas_by_platform()
        fig = px.line(df, x='date', y='roas', color='platform', title="ROAS by Platform Over Time")
        st.plotly_chart(fig)
    elif grouper == "Package Type":
        st.subheader("ROAS by Package Type")
        df = get_roas_by_package_type()
        fig = px.line(df, x='date', y='roas', color='package_type', title="ROAS by Package Type Over Time")
        st.plotly_chart(fig)

# ============================
# Main Function to Render Dashboard
# ============================

def main():
    st.title("Dream Games Dashboard")
    
    # Create tabs for different subject areas
    tab1, tab2, tab3, tab4 = st.tabs([
        "User Engagement", 
        "Monetization Performance", 
        "Retention", 
        "Marketing Effectiveness"
    ])
    
    with tab1:
        user_engagement_tab()
    with tab2:
        monetization_tab()
    with tab3:
        retention_tab()
    with tab4:
        marketing_tab()

if __name__ == '__main__':
    main()
