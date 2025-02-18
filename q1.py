import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery as bq
from bq_manager import BQManager


# ---------------------------
# Dashboard Class
# ---------------------------
class Dashboard:
    def __init__(self, bq_manager):
        # bq_manager is an instance of BigQueryOperations (or your BQManager)
        self.bq_manager = bq_manager

    def user_engagement_tab(self):
        st.header("User Engagement")
        # Unique key for this selectbox
        grouper = st.selectbox(
            "Select Grouper", options=["None", "Platform"],
            index=0, key="user_engagement_grouper"
        )
        if grouper == "None":
            st.subheader("Total Daily Active Users (DAU)")
            df = self.bq_manager.get_total_dau()
            fig = px.line(df, x='date', y='dau', title="Total DAU Over Time")
            st.plotly_chart(fig)
        elif grouper == "Platform":
            st.subheader("DAU by Platform")
            df = self.bq_manager.get_dau_by_platform()
            fig = px.line(df, x='date', y='dau', color='platform',
                          title="DAU by Platform Over Time")
            st.plotly_chart(fig)

    def monetization_tab(self):
        st.header("Monetization Performance")
        grouper = st.selectbox(
            "Select Grouper", options=["None", "Platform", "Package Type"],
            index=0, key="monetization_grouper"
        )
        if grouper == "None":
            st.subheader("Overall ARPDAU")
            df = self.bq_manager.get_overall_arpdau()
            fig = px.line(df, x='date', y='arpdau', title="Overall ARPDAU Over Time")
            st.plotly_chart(fig)
        elif grouper == "Platform":
            st.subheader("ARPDAU by Platform")
            df = self.bq_manager.get_arpdau_by_platform()
            fig = px.line(df, x='date', y='arpdau', color='platform',
                          title="ARPDAU by Platform Over Time")
            st.plotly_chart(fig)
        elif grouper == "Package Type":
            st.subheader("ARPDAU by Package Type")
            df = self.bq_manager.get_arpdau_by_package_type()
            fig = px.line(df, x='date', y='arpdau', color='package_type',
                          title="ARPDAU by Package Type Over Time")
            st.plotly_chart(fig)

    def retention_tab(self):
        st.header("Retention")
        grouper = st.selectbox(
            "Select Grouper", options=["None", "Platform"],
            index=0, key="retention_grouper"
        )
        if grouper == "None":
            st.subheader("Overall Next-Day Retention")
            df = self.bq_manager.get_overall_retention()
            fig = px.line(df, x='install_date', y='retention_rate',
                          title="Overall Next-Day Retention Over Time")
            st.plotly_chart(fig)
        elif grouper == "Platform":
            st.subheader("Next-Day Retention by Platform")
            df = self.bq_manager.get_retention_by_platform()
            fig = px.line(df, x='install_date', y='retention_rate', color='platform',
                          title="Next-Day Retention by Platform Over Time")
            st.plotly_chart(fig)

    def marketing_tab(self):
        st.header("Marketing Effectiveness & Acquisition")
        grouper = st.selectbox(
            "Select Grouper", options=["None", "Platform", "Package Type"],
            index=0, key="marketing_grouper"
        )
        if grouper == "None":
            st.subheader("Overall ROAS")
            df = self.bq_manager.get_overall_roas()
            fig = px.line(df, x='date', y='roas', title="Overall ROAS Over Time")
            st.plotly_chart(fig)
        elif grouper == "Platform":
            st.subheader("ROAS by Platform")
            df = self.bq_manager.get_roas_by_platform()
            fig = px.line(df, x='date', y='roas', color='platform',
                          title="ROAS by Platform Over Time")
            st.plotly_chart(fig)
        elif grouper == "Package Type":
            st.subheader("ROAS by Package Type")
            df = self.bq_manager.get_roas_by_package_type()
            fig = px.line(df, x='date', y='roas', color='package_type',
                          title="ROAS by Package Type Over Time")
            st.plotly_chart(fig)

    def render(self):
        st.title("Dream Games Dashboard")
        # Create tabs for different subject areas
        tab1, tab2, tab3, tab4 = st.tabs([
            "User Engagement", 
            "Monetization Performance", 
            "Retention", 
            "Marketing Effectiveness"
        ])
        with tab1:
            self.user_engagement_tab()
        with tab2:
            self.monetization_tab()
        with tab3:
            self.retention_tab()
        with tab4:
            self.marketing_tab()

# ---------------------------
# Main Execution
# ---------------------------
if __name__ == '__main__':
    SERVICE_ACCOUNT_PATH = '/home/yusuf/DataScience/dream_games/ybektas20.json'
    bq_manager = BQManager(SERVICE_ACCOUNT_PATH)
    dashboard = Dashboard(bq_manager)
    dashboard.render()