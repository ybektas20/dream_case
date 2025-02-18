from google.cloud import bigquery as bq

class BQManager:
    def __init__(self, service_account_path):
        # Initialize the BigQuery client using your service account JSON
        self.client = bq.Client.from_service_account_json(service_account_path)
        
        # Initialize cache variables for each query's DataFrame
        self.total_dau_df = None
        self.dau_by_platform_df = None
        self.overall_arpdau_df = None
        self.arpdau_by_platform_df = None
        self.arpdau_by_package_type_df = None
        self.overall_retention_df = None
        self.retention_by_platform_df = None
        self.overall_roas_df = None
        self.roas_by_platform_df = None
        self.roas_by_package_type_df = None

    def get_total_dau(self):
        if self.total_dau_df is None:
            query = """
            SELECT DATE(event_time) AS date,
                   COUNT(DISTINCT user_id) AS dau
            FROM `casedreamgames.case_db.q1_table_session`
            GROUP BY date
            ORDER BY date;
            """
            self.total_dau_df = self.client.query(query).result().to_dataframe()
        return self.total_dau_df

    def get_dau_by_platform(self):
        if self.dau_by_platform_df is None:
            query = """
            SELECT DATE(event_time) AS date,
                   platform,
                   COUNT(DISTINCT user_id) AS dau
            FROM `casedreamgames.case_db.q1_table_session`
            GROUP BY date, platform
            ORDER BY platform, date;
            """
            self.dau_by_platform_df = self.client.query(query).result().to_dataframe()
        return self.dau_by_platform_df

    def get_overall_arpdau(self):
        if self.overall_arpdau_df is None:
            query = """
            WITH daily_revenue AS (
              SELECT DATE(event_time) AS date, 
                     SUM(CAST(revenue AS FLOAT64)) AS total_revenue
              FROM `casedreamgames.case_db.q1_table_revenue`
              GROUP BY date
            ),
            daily_active AS (
              SELECT DATE(event_time) AS date, 
                     COUNT(DISTINCT user_id) AS dau
              FROM `casedreamgames.case_db.q1_table_session`
              GROUP BY date
            )
            SELECT dr.date,
                   dr.total_revenue / da.dau AS arpdau
            FROM daily_revenue dr
            JOIN daily_active da ON dr.date = da.date
            ORDER BY dr.date;
            """
            self.overall_arpdau_df = self.client.query(query).result().to_dataframe()
        return self.overall_arpdau_df

    def get_arpdau_by_platform(self):
        if self.arpdau_by_platform_df is None:
            query = """
            WITH revenue_platform AS (
              SELECT DATE(event_time) AS date, 
                     platform, 
                     SUM(CAST(revenue AS FLOAT64)) AS total_revenue
              FROM `casedreamgames.case_db.q1_table_revenue`
              GROUP BY date, platform
            ),
            dau_platform AS (
              SELECT DATE(event_time) AS date, 
                     platform, 
                     COUNT(DISTINCT user_id) AS dau
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
            self.arpdau_by_platform_df = self.client.query(query).result().to_dataframe()
        return self.arpdau_by_platform_df

    def get_arpdau_by_package_type(self):
        if self.arpdau_by_package_type_df is None:
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
            self.arpdau_by_package_type_df = self.client.query(query).result().to_dataframe()
        return self.arpdau_by_package_type_df

    def get_overall_retention(self):
        if self.overall_retention_df is None:
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
            self.overall_retention_df = self.client.query(query).result().to_dataframe()
        return self.overall_retention_df

    def get_retention_by_platform(self):
        if self.retention_by_platform_df is None:
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
            self.retention_by_platform_df = self.client.query(query).result().to_dataframe()
        return self.retention_by_platform_df

    def get_overall_roas(self):
        if self.overall_roas_df is None:
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
            self.overall_roas_df = self.client.query(query).result().to_dataframe()
        return self.overall_roas_df

    def get_roas_by_platform(self):
        if self.roas_by_platform_df is None:
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
            self.roas_by_platform_df = self.client.query(query).result().to_dataframe()
        return self.roas_by_platform_df

    def get_roas_by_package_type(self):
        if self.roas_by_package_type_df is None:
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
            self.roas_by_package_type_df = self.client.query(query).result().to_dataframe()
        return self.roas_by_package_type_df
