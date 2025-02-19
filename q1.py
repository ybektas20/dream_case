import streamlit as st
import pandas as pd
import plotly.express as px
import concurrent.futures

from DreamGamesAnalytics import DreamGamesAnalytics

SERVICE_ACCOUNT_PATH = "/home/yusuf/DataScience/dream_games/ybektas20.json"

# --------------------------------------------------------------------
# 1) Define how to categorize queries
# --------------------------------------------------------------------
def get_query_catalog(analytics):
    return {
        "ARPDAU": {
            "arpdau_by_groups": analytics.arpdau_by_groups,
            "arpdau_by_package_type": analytics.arpdau_by_package_type,
            "arpdau_network_based": analytics.arpdau_network_based,
            "arpdau_per_level": analytics.arpdau_per_level,
            "arpdau_per_platform": analytics.arpdau_per_platform,
            "arpdau_trend": analytics.arpdau_trend,
        },
        "ARPU": {
            "arpu_based_on_level_progression": analytics.arpu_based_on_level_progression,
            "arpu_by_groups": analytics.arpu_by_groups,
            "arpu_by_package_type": analytics.arpu_by_package_type,
            "arpu_by_user_cohort": analytics.arpu_by_user_cohort,
            "avg_arpu_calculate": analytics.avg_arpu_calculate,
            "daily_arpu_calculate": analytics.daily_arpu_calculate,
            "network_based_arpu": analytics.network_based_arpu
        },
        "DAU": {
            "dau": analytics.dau,
            "dau_trend": analytics.dau_trend,
            "dau_by_groups": analytics.dau_by_groups,
            "dau_by_package_type": analytics.dau_by_package_type,
            "dau_network_based": analytics.dau_network_based,
            "dau_per_level": analytics.dau_per_level,
            "dau_per_platform": analytics.dau_per_platform
        },
        "Retention": {
            "retention_trend": analytics.retention_trend,
            "retention_by_groups": analytics.retention_by_groups,
            "retention_by_package_type": analytics.retention_by_package_type,
            "retention_network_based": analytics.retention_network_based,
            "retention_per_level": analytics.retention_per_level,
            "retention_per_platform": analytics.retention_per_platform
        },
        "ROAS": {
            "roas_trend": analytics.roas_trend,
            "roas_by_package_type": analytics.roas_by_package_type,
            "roas_network_based": analytics.roas_network_based,
            "roas_per_level": analytics.roas_per_level,
            "roas_per_platform": analytics.roas_per_platform
        },
        "Other": {
            "mau": analytics.mau,
            "conversion_rate_by_groups": analytics.conversion_rate_by_groups,
            "avg_session_duration_by_groups": analytics.avg_session_duration_by_groups,
            "user_count_of_groups": analytics.user_count_of_groups,
            # placeholders:
            "dream_games_analysis": analytics.dream_games_analysis,
            "ml": analytics.ml,
            "predictive_modelling": analytics.predictive_modelling,
        }
    }

# --------------------------------------------------------------------
# 2) Use concurrency to load queries in parallel
# --------------------------------------------------------------------
@st.cache_data
def load_all_queries_concurrently():
    analytics = DreamGamesAnalytics(SERVICE_ACCOUNT_PATH)
    query_catalog = get_query_catalog(analytics)

    results = {}
    jobs = []
    for cat, queries in query_catalog.items():
        for name, func in queries.items():
            jobs.append((cat, name, func))

    for cat in query_catalog:
        results[cat] = {}
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_job = {}
        for (cat, qname, func) in jobs:
            future = executor.submit(func)
            future_to_job[future] = (cat, qname)

        for future in concurrent.futures.as_completed(future_to_job):
            (cat, qname) = future_to_job[future]
            try:
                df = future.result()
            except Exception as e:
                df = None
                print(f"Query {qname} in category {cat} failed: {e}")
            results[cat][qname] = df

    return results

# --------------------------------------------------------------------
# 3) The main Streamlit app
# --------------------------------------------------------------------
def main():
    st.title("Dream Games Analytics Dashboard")

    # 3.1) Load everything upfront, in parallel
    st.info("Loading all queries in parallel. Please wait...")
    results = load_all_queries_concurrently()
    st.success("All queries loaded successfully!")
    
    # 3.2) Show categories in top-level tabs
    category_names = list(results.keys())
    tabs = st.tabs(category_names)
    
    for tab, cat_name in zip(tabs, category_names):
        with tab:
            st.subheader(cat_name)
            # Let the user pick which query to display
            sub_query_names = list(results[cat_name].keys())
            selected_query_name = st.selectbox(
                f"Select a query in {cat_name} category:",
                sub_query_names,
                key=f"{cat_name}_selectbox"
            )

            df = results[cat_name][selected_query_name]
            
            if df is None:
                st.warning(f"No data (or query failed) for: {selected_query_name}")
                continue
            
            # Some queries might be placeholders that do not return data
            if not isinstance(df, pd.DataFrame):
                st.warning(f"This query '{selected_query_name}' is a placeholder and does not return data.")
                continue
            
             # 3.3) Visualization with Plotly
            st.write(f"### {selected_query_name} Visualization")
            
            df_display = df.copy()
            
            # Ensure 'date' is actually datetime if present
            if "date" in df_display.columns:
                df_display["date"] = pd.to_datetime(df_display["date"], errors="coerce")
            
            # Identify numeric and non-numeric (dimension) columns
            numeric_cols = df_display.select_dtypes(include=["number"]).columns.tolist()
            cat_cols = df_display.select_dtypes(exclude=["number", "datetime"]).columns.tolist()
            
            # We'll pick the first numeric column as y-axis for demonstration
            if len(numeric_cols) == 0:
                st.info("No numeric columns found to plot or summarize.")
            else:
                y_col = numeric_cols[0]
            
                # If there's at least one categorical column, we'll also color by it
                if len(cat_cols) > 0:
                    dimension_col = cat_cols[0]
                else:
                    dimension_col = None
            
                # -------- 1) Plot If There's a 'date' Column (Time Series) --------
                if "date" in df_display.columns:
                    if dimension_col:
                        # Multi-line chart: color by the dimension
                        fig = px.line(
                            df_display,
                            x="date", 
                            y=y_col,
                            color=dimension_col,
                            title=f"{y_col} over Time by {dimension_col}"
                        )
                    else:
                        # Single-line chart
                        fig = px.line(
                            df_display,
                            x="date", 
                            y=y_col,
                            title=f"{y_col} over Time"
                        )
            
                    fig.update_layout(xaxis_title="Date", yaxis_title=y_col)
                    st.plotly_chart(fig, use_container_width=True)
            
                # -------- 2) Otherwise, No 'date' Column => Possibly a Bar Chart --------
                else:
                    # If we have a dimension column, do grouped bars
                    if dimension_col:
                        fig = px.bar(
                            df_display,
                            x=dimension_col,
                            y=y_col,
                            title=f"{y_col} by {dimension_col}"
                        )
                        fig.update_layout(xaxis_title=dimension_col, yaxis_title=y_col)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No 'date' column or dimension column found to make a grouped chart. Showing a simple bar:")
                        # Just pick an index-based chart or skip altogether
                        fig = px.bar(df_display, y=y_col, title=f"{y_col} values")
                        st.plotly_chart(fig, use_container_width=True)
            
            # 3.4) Descriptive Statistics
            st.write(f"#### Descriptive Statistics for `{selected_query_name}`")
            
            if len(numeric_cols) == 0:
                st.info("No numeric data to show descriptive statistics.")
            else:
                # If there's a dimension column, we group by it
                if dimension_col:
                    grouped_stats = df_display.groupby(dimension_col)[numeric_cols].describe()
                    # Optionally flatten multi-level columns
                    # grouped_stats.columns = ["_".join(col) for col in grouped_stats.columns]
                    st.dataframe(grouped_stats)
                else:
                    # Standard describe
                    desc_stats = df_display[numeric_cols].describe()
                    st.dataframe(desc_stats)
            
            
if __name__ == "__main__":
    main()
