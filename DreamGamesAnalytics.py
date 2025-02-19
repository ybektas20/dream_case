import pandas as pd
from google.cloud import bigquery as bq

class DreamGamesAnalytics:
    """
    A consolidated analytics class that fetches each named query.
    """
    def __init__(self, service_account_path: str):
        self.client = bq.Client.from_service_account_json(service_account_path)

    def _run_query(self, query: str) -> pd.DataFrame:
        return self.client.query(query).result().to_dataframe()

    # -------------------------------------------------------------------------
    #  1) ARPDAU
    # -------------------------------------------------------------------------
    def arpdau_by_package_type(self) -> pd.DataFrame:
        query = """
        -- Sample logic: sum of daily revenue by package_type / daily DAU 
        WITH revenue_pkg AS (
          SELECT
            DATE(event_time) AS date,
            package_type,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date, package_type
        ),
        daily_active AS (
          SELECT
            DATE(event_time) AS date,
            COUNT(DISTINCT user_id) AS dau
          FROM `casedreamgames.case_db.q1_table_session`
          GROUP BY date
        )
        SELECT
          r.package_type,
          r.date,
          SAFE_DIVIDE(r.total_revenue, d.dau) AS arpdau
        FROM revenue_pkg r
        JOIN daily_active d USING(date)
        ORDER BY r.package_type, r.date
        """
        return self._run_query(query)

    def arpdau_by_network(self) -> pd.DataFrame:
        query = """
        -- ARPDAU by network
        WITH user_attrs AS (
          SELECT user_id, network
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        daily_revenue AS (
          SELECT 
            user_id,
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id, date
        ),
        daily_active AS (
          SELECT user_id, DATE(event_time) AS date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          COALESCE(u.network, 'unknown') AS network,
          da.date,
          SUM(dr.total_revenue) AS total_revenue,
          COUNT(DISTINCT da.user_id) AS dau,
          SAFE_DIVIDE(SUM(dr.total_revenue), COUNT(DISTINCT da.user_id)) AS arpdau
        FROM daily_active da
        LEFT JOIN user_attrs u ON da.user_id = u.user_id
        LEFT JOIN daily_revenue dr 
               ON da.user_id = dr.user_id
              AND da.date = dr.date
        GROUP BY u.network, da.date
        ORDER BY da.date, network
        """
        return self._run_query(query)

    def arpdau_by_level(self) -> pd.DataFrame:
        query = """
        WITH user_daily_level AS (
          SELECT
            user_id,
            DATE(event_time) AS date,
            level
          FROM `casedreamgames.case_db.q1_table_session`
          GROUP BY user_id, DATE(event_time), level
        ),
        daily_rev AS (
          SELECT
            user_id,
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS daily_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id, date
        ),
        combined AS (
          SELECT
            COALESCE(udl.level, -1) AS level,
            COALESCE(dr.daily_revenue, 0) AS daily_revenue,
            1 AS daily_active
          FROM user_daily_level udl
          LEFT JOIN daily_rev dr
                 ON udl.user_id = dr.user_id
                AND udl.date = dr.date
        )
        SELECT
          level,
          SUM(daily_revenue) AS total_revenue,
          SUM(daily_active)  AS total_dau_days,
          SAFE_DIVIDE(SUM(daily_revenue), SUM(daily_active)) AS arpdau
        FROM combined
        GROUP BY level
        ORDER BY level
        """
        return self._run_query(query)

    def arpdau_by_platform(self) -> pd.DataFrame:
        query = """
        -- ARPDAU by platform
        WITH daily_rev AS (
          SELECT
            DATE(r.event_time) AS date,
            SUM(CAST(r.revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue` r
          GROUP BY date
        ),
        daily_active AS (
          SELECT
            DATE(s.event_time) AS date,
            platform,
            COUNT(DISTINCT s.user_id) AS dau
          FROM `casedreamgames.case_db.q1_table_session` s
          GROUP BY date, platform
        )
        SELECT
          d.platform,
          d.date,
          SAFE_DIVIDE(r.total_revenue, d.dau) AS arpdau
        FROM daily_rev r
        JOIN daily_active d USING(date)
        ORDER BY d.platform, d.date
        """
        return self._run_query(query)

    def arpdau_trend(self) -> pd.DataFrame:
        query = """
        -- ARPDAU (no grouping)
        WITH daily_revenue AS (
          SELECT
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date
        ),
        daily_active AS (
          SELECT
            DATE(event_time) AS date,
            COUNT(DISTINCT user_id) AS dau
          FROM `casedreamgames.case_db.q1_table_session`
          GROUP BY date
        )
        SELECT
          dr.date,
          dr.total_revenue / da.dau AS arpdau
        FROM daily_revenue dr
        JOIN daily_active da USING(date)
        ORDER BY dr.date
        """
        return self._run_query(query)

    # -------------------------------------------------------------------------
    #  2) ARPU
    # -------------------------------------------------------------------------
    def arpu_by_level_progression(self) -> pd.DataFrame:
        query = """
        WITH max_level AS (
          SELECT
            user_id,
            MAX(level) AS max_level
          FROM `casedreamgames.case_db.q1_table_level_end`
          GROUP BY user_id
        ),
        user_rev AS (
          SELECT
            user_id,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id
        )
        SELECT
          m.max_level,
          COUNT(DISTINCT m.user_id) AS user_count,
          SUM(u.total_revenue) AS total_revenue,
          SAFE_DIVIDE(SUM(u.total_revenue), COUNT(DISTINCT m.user_id)) AS arpu
        FROM max_level m
        LEFT JOIN user_rev u ON m.user_id = u.user_id
        GROUP BY m.max_level
        ORDER BY m.max_level
        """
        return self._run_query(query)

    def arpu_by_package_type(self) -> pd.DataFrame:
        query = """
        WITH pkg_revenue AS (
          SELECT
            user_id,
            package_type,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id, package_type
        ),
        total_users AS (
          SELECT COUNT(DISTINCT user_id) AS total_users
          FROM `casedreamgames.case_db.q1_table_install`
        )
        SELECT
          package_type,
          SUM(total_revenue) AS total_revenue,
          t.total_users,
          SAFE_DIVIDE(SUM(total_revenue), t.total_users) AS arpu
        FROM pkg_revenue pr
        CROSS JOIN total_users t
        GROUP BY package_type, t.total_users
        ORDER BY package_type
        """
        return self._run_query(query)

    def arpu_by_user_cohort(self) -> pd.DataFrame:
        query = """
        WITH user_cohort AS (
          SELECT
            user_id,
            EXTRACT(YEAR FROM event_time) AS install_year,
            EXTRACT(MONTH FROM event_time) AS install_month
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        user_rev AS (
          SELECT
            user_id,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id
        )
        SELECT
          install_year,
          install_month,
          COUNT(DISTINCT c.user_id) AS cohort_size,
          SUM(r.total_revenue) AS total_revenue,
          SAFE_DIVIDE(SUM(r.total_revenue), COUNT(DISTINCT c.user_id)) AS arpu
        FROM user_cohort c
        LEFT JOIN user_rev r ON c.user_id = r.user_id
        GROUP BY install_year, install_month
        ORDER BY install_year, install_month
        """
        return self._run_query(query)
                               

    def arpu_trend(self) -> pd.DataFrame:
        query = """
        WITH daily_revenue AS (
          SELECT
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date
        ),
        total_users AS (
          SELECT
            DATE(event_time) AS date,
            COUNT(DISTINCT user_id) 
              OVER(ORDER BY DATE(event_time) 
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_users
          FROM `casedreamgames.case_db.q1_table_install`
        )
        SELECT
          r.date,
          SAFE_DIVIDE(r.total_revenue, u.cum_users) AS arpu
        FROM daily_revenue r
        JOIN total_users u USING(date)
        ORDER BY r.date
        """
        return self._run_query(query)

    def arpu_by_network(self) -> pd.DataFrame:
        """
        Possibly in your list twice. 
        We'll do total revenue by network / total users by network = ARPU by network.
        """
        query = """
        WITH install_attrs AS (
          SELECT user_id, network
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        user_rev AS (
          SELECT user_id, SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY user_id
        )
        SELECT
          COALESCE(i.network, 'unknown') AS network,
          COUNT(DISTINCT i.user_id) AS user_count,
          SUM(u.total_revenue) AS total_revenue,
          SAFE_DIVIDE(SUM(u.total_revenue), COUNT(DISTINCT i.user_id)) AS arpu
        FROM install_attrs i
        LEFT JOIN user_rev u ON i.user_id = u.user_id
        GROUP BY network
        ORDER BY network
        """
        return self._run_query(query)

    # -------------------------------------------------------------------------
    #  3) DAU
    # -------------------------------------------------------------------------
    def dau_by_package_type(self) -> pd.DataFrame:
        query = """
        -- As previously discussed, we interpret "dau_by_package_type" 
        -- as the daily active users who purchased or are associated with a package type.
        WITH user_pkg AS (
            SELECT DISTINCT user_id, package_type
            FROM `casedreamgames.case_db.q1_table_revenue`
        ),
        session_data AS (
            SELECT DATE(event_time) AS date, user_id
            FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT 
          s.date,
          COALESCE(u.package_type, 'no_purchase') AS package_type,
          COUNT(DISTINCT s.user_id) AS dau
        FROM session_data s
        LEFT JOIN user_pkg u ON s.user_id = u.user_id
        GROUP BY s.date, u.package_type
        ORDER BY s.date, package_type
        """
        return self._run_query(query)

    def dau_by_network(self) -> pd.DataFrame:
        query = """
        -- DAU by network
        WITH session_data AS (
            SELECT DATE(event_time) AS date, user_id
            FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          s.date,
          COALESCE(i.network, 'unknown') AS network,
          COUNT(DISTINCT s.user_id) AS dau
        FROM session_data s
        LEFT JOIN `casedreamgames.case_db.q1_table_install` i
               ON s.user_id = i.user_id
        GROUP BY s.date, network
        ORDER BY s.date, network
        """
        return self._run_query(query)
    
    def dau_by_country(self) -> pd.DataFrame:
        query="""
        -- DAU by country
        WITH session_data AS (
            SELECT DATE(event_time) AS date, user_id
            FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          s.date,
          COALESCE(i.country, 'unknown') AS country,
          COUNT(DISTINCT s.user_id) AS dau
        FROM session_data s
        LEFT JOIN `casedreamgames.case_db.q1_table_install` i
               ON s.user_id = i.user_id
        GROUP BY s.date, country
        ORDER BY s.date, country        
        """
        return self._run_query(query)

    def dau_by_platform(self) -> pd.DataFrame:
        query = """
        SELECT
          DATE(event_time) AS date,
          platform,
          COUNT(DISTINCT user_id) AS dau
        FROM `casedreamgames.case_db.q1_table_session`
        GROUP BY date, platform
        ORDER BY date, platform
        """
        return self._run_query(query)

    def dau_trend(self) -> pd.DataFrame:
        query = """
        SELECT
          DATE(event_time) AS date,
          COUNT(DISTINCT user_id) AS dau
        FROM `casedreamgames.case_db.q1_table_session`
        GROUP BY date
        ORDER BY date
        """
        return self._run_query(query)

    # -------------------------------------------------------------------------
    #  4) Retention
    # -------------------------------------------------------------------------
    def retention_by_groups(self) -> pd.DataFrame:
        query = """
        -- Day-1 retention by (platform, network, country)
        WITH installs AS (
          SELECT 
            user_id,
            platform,
            network,
            country,
            DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          COALESCE(i.platform, 'unknown') AS platform,
          COALESCE(i.network, 'unknown')  AS network,
          COALESCE(i.country, 'unknown')  AS country,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY platform, network, country, i.install_date
        ORDER BY i.install_date, platform, network, country
        """
        return self._run_query(query)

    def retention_by_package_type(self) -> pd.DataFrame:
        query = """
        -- as Day-1 retention among users who have or haven't purchased certain package types
        WITH installs AS (
          SELECT user_id, DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        user_pkg AS (
          SELECT DISTINCT user_id, package_type
          FROM `casedreamgames.case_db.q1_table_revenue`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          COALESCE(u.package_type, 'no_purchase') AS package_type,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN user_pkg u ON i.user_id = u.user_id
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY package_type, i.install_date
        ORDER BY i.install_date, package_type
        """
        return self._run_query(query)

    def retention_by_network(self) -> pd.DataFrame:
        query = """
        WITH installs AS (
          SELECT user_id, network, DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          COALESCE(i.network, 'unknown') AS network,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY network, i.install_date
        ORDER BY i.install_date, network
        """
        return self._run_query(query)

    def retention_by_level(self) -> pd.DataFrame:
        query = """
        -- Example "retention_by_level" interpretation
        WITH day0_level AS (
          SELECT
            user_id,
            MAX(level) AS level_on_install_day,
            DATE(event_time) AS date
          FROM `casedreamgames.case_db.q1_table_session`
          GROUP BY user_id, date
        ),
        installs AS (
          SELECT user_id, DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          d.level_on_install_day,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN day0_level d
               ON i.user_id = d.user_id
              AND i.install_date = d.date
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY d.level_on_install_day, i.install_date
        ORDER BY i.install_date, d.level_on_install_day
        """
        return self._run_query(query)

    def retention_by_platform(self) -> pd.DataFrame:
        query = """
        WITH installs AS (
          SELECT user_id, platform, DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          COALESCE(i.platform, 'unknown') AS platform,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY platform, i.install_date
        ORDER BY i.install_date, platform
        """
        return self._run_query(query)

    def retention_trend(self) -> pd.DataFrame:
        query = """
        WITH installs AS (
          SELECT user_id, DATE(event_time) AS install_date
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        sessions AS (
          SELECT user_id, DATE(event_time) AS session_date
          FROM `casedreamgames.case_db.q1_table_session`
        )
        SELECT
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY i.install_date
        ORDER BY i.install_date
        """
        return self._run_query(query)

    # -------------------------------------------------------------------------
    #  5) ROAS
    # -------------------------------------------------------------------------
    def roas_by_package_type(self) -> pd.DataFrame:
        query = """
        -- Because cost table doesn't have package_type, 
        -- we do lumpsum cost per day, revenue by package_type per day.
        WITH daily_rev AS (
          SELECT
            DATE(event_time) AS date,
            package_type,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date, package_type
        ),
        daily_cost AS (
          SELECT
            date,
            SUM(cost) AS total_cost
          FROM `casedreamgames.case_db.q1_table_cost`
          GROUP BY date
        )
        SELECT
          r.date,
          r.package_type,
          r.total_revenue,
          c.total_cost,
          SAFE_DIVIDE(r.total_revenue, c.total_cost) AS roas
        FROM daily_rev r
        JOIN daily_cost c USING(date)
        ORDER BY package_type, date
        """
        return self._run_query(query)

    def roas_by_network(self) -> pd.DataFrame:
        query = """
        WITH daily_rev AS (
          SELECT
            DATE(r.event_time) AS date,
            i.network,
            SUM(CAST(r.revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue` r
          JOIN `casedreamgames.case_db.q1_table_install` i
            ON r.user_id = i.user_id
          GROUP BY date, i.network
        ),
        daily_cost AS (
          SELECT 
            date,
            network,
            SUM(cost) AS total_cost
          FROM `casedreamgames.case_db.q1_table_cost`
          GROUP BY date, network
        )
        SELECT
          r.date,
          COALESCE(r.network, 'unknown') AS network,
          r.total_revenue,
          c.total_cost,
          SAFE_DIVIDE(r.total_revenue, c.total_cost) AS roas
        FROM daily_rev r
        JOIN daily_cost c
          ON r.date = c.date
         AND r.network = c.network
        ORDER BY r.date, network
        """
        return self._run_query(query)

    def roas_by_level(self) -> pd.DataFrame:
        query = """
        -- Lumpsum cost per day, revenue by day+level
        WITH daily_rev AS (
          SELECT 
            DATE(le.event_time) AS date,
            le.level,
            SUM(CAST(r.revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue` r
          JOIN `casedreamgames.case_db.q1_table_level_end` le
            ON r.user_id = le.user_id
           AND DATE(r.event_time) = DATE(le.event_time)
          GROUP BY date, le.level
        ),
        daily_cost AS (
          SELECT
            date,
            SUM(cost) AS total_cost
          FROM `casedreamgames.case_db.q1_table_cost`
          GROUP BY date
        )
        SELECT
          r.date,
          r.level,
          r.total_revenue,
          c.total_cost,
          SAFE_DIVIDE(r.total_revenue, c.total_cost) AS roas
        FROM daily_rev r
        JOIN daily_cost c USING(date)
        ORDER BY r.date, r.level
        """
        return self._run_query(query)

    def roas_by_platform(self) -> pd.DataFrame:
        query = """
        WITH daily_rev AS (
          SELECT
            DATE(event_time) AS date,
            platform,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date, platform
        ),
        daily_cost AS (
          SELECT
            date,
            platform,
            SUM(cost) AS total_cost
          FROM `casedreamgames.case_db.q1_table_cost`
          GROUP BY date, platform
        )
        SELECT
          r.date,
          COALESCE(r.platform, 'unknown') AS platform,
          r.total_revenue,
          c.total_cost,
          SAFE_DIVIDE(r.total_revenue, c.total_cost) AS roas
        FROM daily_rev r
        JOIN daily_cost c
               ON r.date = c.date
              AND r.platform = c.platform
        ORDER BY r.date, platform
        """
        return self._run_query(query)

    def roas_trend(self) -> pd.DataFrame:
        query = """
        WITH daily_rev AS (
          SELECT
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date
        ),
        daily_cost AS (
          SELECT
            date,
            SUM(cost) AS total_cost
          FROM `casedreamgames.case_db.q1_table_cost`
          GROUP BY date
        )
        SELECT
          r.date,
          r.total_revenue,
          c.total_cost,
          SAFE_DIVIDE(r.total_revenue, c.total_cost) AS roas
        FROM daily_rev r
        JOIN daily_cost c USING(date)
        ORDER BY r.date
        """
        return self._run_query(query)

    # -------------------------------------------------------------------------
    #  6) Other (MAU, conversion_rate, etc.)
    # -------------------------------------------------------------------------
    def mau(self) -> pd.DataFrame:
        query = """
        SELECT
          EXTRACT(YEAR FROM event_time) AS year,
          EXTRACT(MONTH FROM event_time) AS month,
          COUNT(DISTINCT user_id) AS mau
        FROM `casedreamgames.case_db.q1_table_session`
        GROUP BY year, month
        ORDER BY year, month
        """
        return self._run_query(query)

    def user_count_of_groups(self) -> pd.DataFrame:
        query = """
        SELECT
          COALESCE(platform, 'unknown') AS platform,
          COALESCE(network, 'unknown') AS network,
          COALESCE(country, 'unknown') AS country,
          COUNT(DISTINCT user_id) AS user_count
        FROM `casedreamgames.case_db.q1_table_install`
        GROUP BY platform, network, country
        ORDER BY platform, network, country
        """
        return self._run_query(query)

    def cost(self) -> pd.DataFrame:
        query = """
        SELECT * FROM `casedreamgames.case_db.q1_table_cost`
        
        """
        return self._run_query(query)   

    def avg_session_duration_by_groups(self) -> pd.DataFrame:
        query = """
        WITH sess AS (
          SELECT 
            user_id,
            platform AS session_platform,
            time_spent
          FROM `casedreamgames.case_db.q1_table_session`
        ),
        install_attrs AS (
          SELECT DISTINCT user_id, platform, network, country
          FROM `casedreamgames.case_db.q1_table_install`
        )
        SELECT
          COALESCE(i.platform, 'unknown')  AS install_platform,
          COALESCE(i.network,  'unknown')  AS network,
          COALESCE(i.country,  'unknown')  AS country,
          AVG(s.time_spent) AS avg_session_duration
        FROM sess s
        LEFT JOIN install_attrs i ON s.user_id = i.user_id
        GROUP BY install_platform, network, country
        ORDER BY install_platform, network, country
        """
        return self._run_query(query)

    def conversion_rate_by_groups(self) -> pd.DataFrame:
        query = """
        -- (# of payers / total users) by platform, network, country
        WITH user_attrs AS (
          SELECT user_id, platform, network, country
          FROM `casedreamgames.case_db.q1_table_install`
        ),
        payers AS (
          SELECT DISTINCT user_id
          FROM `casedreamgames.case_db.q1_table_revenue`
        )
        SELECT
          COALESCE(u.platform, 'unknown') AS platform,
          COALESCE(u.network, 'unknown')  AS network,
          COALESCE(u.country, 'unknown')  AS country,
          COUNT(DISTINCT u.user_id) AS total_users,
          COUNT(DISTINCT CASE WHEN p.user_id IS NOT NULL THEN u.user_id END) AS payers,
          SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN p.user_id IS NOT NULL THEN u.user_id END),
            COUNT(DISTINCT u.user_id)
          ) AS conversion_rate
        FROM user_attrs u
        LEFT JOIN payers p ON u.user_id = p.user_id
        GROUP BY platform, network, country
        ORDER BY platform, network, country
        """
        return self._run_query(query)

