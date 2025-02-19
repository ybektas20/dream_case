import pandas as pd
from google.cloud import bigquery as bq

class DreamGamesAnalytics:
    """
    A consolidated analytics class that fetches named queries
    for Dream Games data, grouped by logical KPIs and metrics.
    """
    def __init__(self, service_account_path: str):
        self.client = bq.Client.from_service_account_json(service_account_path)

    def _run_query(self, query: str) -> pd.DataFrame:
        return self.client.query(query).result().to_dataframe()

    # =========================================================================
    #  1) USER ACQUISITION & DAU
    # =========================================================================
    def get_dau_trend(self) -> pd.DataFrame:
        """
        Returns daily active users (DAU) over time (date).
        """
        query = """
        SELECT
          DATE(event_time) AS date,
          COUNT(DISTINCT user_id) AS dau
        FROM `casedreamgames.case_db.q1_table_session`
        GROUP BY date
        ORDER BY date
        """
        return self._run_query(query)

    def get_dau_by_country(self) -> pd.DataFrame:
        """
        Returns daily active users segmented by country.
        """
        query = """
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

    def get_dau_by_network(self) -> pd.DataFrame:
        """
        Returns daily active users segmented by acquisition network.
        """
        query = """
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

    def get_dau_by_platform(self) -> pd.DataFrame:
        """
        Returns daily active users segmented by platform (iOS/Android).
        """
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

    def get_dau_by_package_type(self) -> pd.DataFrame:
        """
        Returns daily active users segmented by package_type (based on purchases).
        """
        query = """
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

    def get_mau(self) -> pd.DataFrame:
        """
        Returns monthly active users (MAU) by year and month.
        """
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

    # =========================================================================
    #  2) RETENTION
    # =========================================================================
    def get_retention_trend(self) -> pd.DataFrame:
        """
        Returns daily install counts, day-1 retained users, and day-1 retention rate.
        """
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

    def get_retention_by_country(self) -> pd.DataFrame:
        """
        Returns day-1 retention by country.
        """
        query = """
        WITH installs AS (
          SELECT user_id, country, DATE(event_time) AS install_date
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
          COALESCE(i.country, 'unknown') AS country,
          i.install_date,
          COUNT(DISTINCT i.user_id) AS installs,
          COUNT(DISTINCT s.user_id) AS retained_day1,
          SAFE_DIVIDE(COUNT(DISTINCT s.user_id), COUNT(DISTINCT i.user_id)) AS retention_day1
        FROM installs i
        LEFT JOIN user_pkg u ON i.user_id = u.user_id
        LEFT JOIN sessions s
               ON i.user_id = s.user_id
              AND s.session_date = DATE_ADD(i.install_date, INTERVAL 1 DAY)
        GROUP BY country, i.install_date
        ORDER BY i.install_date, country
        """
        return self._run_query(query)

    def get_retention_by_network(self) -> pd.DataFrame:
        """
        Returns day-1 retention segmented by network.
        """
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

    def get_retention_by_platform(self) -> pd.DataFrame:
        """
        Returns day-1 retention segmented by platform.
        """
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

    def get_retention_by_package_type(self) -> pd.DataFrame:
        """
        Returns day-1 retention segmented by package_type (purchase behavior).
        """
        query = """
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

    # =========================================================================
    #  3) MONETIZATION (ARPU / ARPDAU)
    # =========================================================================
    def get_arpdau_trend(self) -> pd.DataFrame:
        """
        Returns daily ARPDAU (no grouping).
        """
        query = """
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

    def get_arpdau_by_package_type(self) -> pd.DataFrame:
        """
        Returns ARPDAU grouped by package_type over time.
        """
        query = """
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

    def get_arpdau_by_network(self) -> pd.DataFrame:
        """
        Returns ARPDAU grouped by network over time.
        """
        query = """
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

    def get_arpdau_by_platform(self) -> pd.DataFrame:
        """
        Returns ARPDAU grouped by platform over time.
        """
        query = """
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

    def get_arpdau_by_level(self) -> pd.DataFrame:
        """
        Returns ARPDAU grouped by level (session level) over time.
        """
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

    def get_arpu_trend(self) -> pd.DataFrame:
        """
        Returns a daily ARPU time series, dividing total daily revenue by the cumulative user base.
        """
        query = """
        WITH daily_revenue AS (
          SELECT
            DATE(event_time) AS date,
            SUM(CAST(revenue AS FLOAT64)) AS total_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date
        ),
        daily_new_users AS (
          -- How many distinct new installers on each date
          SELECT
            DATE(event_time) AS date,
            COUNT(DISTINCT user_id) AS daily_installs
          FROM `casedreamgames.case_db.q1_table_install`
          GROUP BY date
        ),
        cumulative_installs AS (
          -- Running sum of daily installs
          SELECT
            date,
            SUM(daily_installs) OVER (
              ORDER BY date 
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_users
          FROM daily_new_users
        )
        SELECT
          r.date,
          SAFE_DIVIDE(r.total_revenue, ci.cum_users) AS arpu
        FROM daily_revenue r
        JOIN cumulative_installs ci USING(date)
        ORDER BY r.date;
        
        """
        return self._run_query(query)

    def get_arpu_by_level_progression(self) -> pd.DataFrame:
        """
        Returns ARPU segmented by users' maximum level progression.
        """
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

    def get_arpu_by_package_type(self) -> pd.DataFrame:
        """
        Returns ARPU segmented by package_type.
        """
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

    def get_arpu_by_user_cohort(self) -> pd.DataFrame:
        """
        Returns ARPU by install cohort (year & month).
        """
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

    def get_arpu_by_network(self) -> pd.DataFrame:
        """
        Returns ARPU segmented by network.
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

    # =========================================================================
    #  4) COST & ROAS
    # =========================================================================
    def get_cost(self) -> pd.DataFrame:
        """
        Returns cost data from q1_table_cost (all rows),
        ordered by date, platform, network, country.
        """
        query = """
        SELECT * 
        FROM `casedreamgames.case_db.q1_table_cost`
        ORDER BY date, platform, network, country
        """
        return self._run_query(query)

    def get_cost_per_install(self) -> pd.DataFrame:
        """
        Returns a daily joined table of (date, country, platform, network, installs, total_cost).
        """
        query =  """
        WITH daily_installs AS (
            SELECT
                DATE(event_time) AS date,
                country,
                platform,
                network,
                COUNT(DISTINCT user_id) AS installs
            FROM `casedreamgames.case_db.q1_table_install`
            GROUP BY 1, 2, 3, 4
        ),        
        daily_cost AS (
            SELECT
                date,
                country,
                platform,
                network,
                SUM(cost) AS total_cost
            FROM `casedreamgames.case_db.q1_table_cost`
            GROUP BY 1, 2, 3, 4
        )       
        SELECT
            di.date,
            di.country,
            di.platform,
            di.network,
            di.installs,
            COALESCE(dc.total_cost, 0) AS total_cost
        FROM daily_installs di
        LEFT JOIN daily_cost dc
            ON di.date = dc.date
            AND di.country = dc.country
            AND di.platform = dc.platform
            AND di.network = dc.network
        ORDER BY 
            di.date,
            di.country,
            di.platform,
            di.network;       
        """
        return self._run_query(query)

    def get_roas_trend(self) -> pd.DataFrame:
        """
        Returns daily ROAS over time (total_revenue / total_cost).
        """
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

    def get_roas_by_package_type(self) -> pd.DataFrame:
        """
        Returns ROAS by package_type, joining daily cost lumpsum.
        """
        query = """
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

    def get_roas_by_network(self) -> pd.DataFrame:
        """
        Returns ROAS segmented by network (matching daily revenue to daily cost).
        """
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

    def get_roas_by_level(self) -> pd.DataFrame:
        """
        Returns daily ROAS by level, matching daily cost lumpsum with daily revenue at that level.
        """
        query = """
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

    def get_roas_by_platform(self) -> pd.DataFrame:
        """
        Returns ROAS segmented by platform.
        """
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

    # =========================================================================
    #  5) USER ENGAGEMENT (TIME SPENT, LEVELS, ETC.)
    # =========================================================================
    def get_avg_time_spent(self) -> pd.DataFrame:
        """
        Returns average total session time per user, 
        grouped by day_of_week, hour_of_day, and country.
        """
        query = """
        WITH user_hourly AS (
          SELECT
            EXTRACT(DAYOFWEEK FROM s.event_time) AS day_of_week,
            EXTRACT(HOUR FROM s.event_time)      AS hour_of_day,
            i.country,
            s.user_id,
            SUM(s.time_spent) AS total_time_spent
          FROM `casedreamgames.case_db.q1_table_session` s
          JOIN `casedreamgames.case_db.q1_table_install` i
            ON s.user_id = i.user_id
          GROUP BY 
            day_of_week,
            hour_of_day,
            i.country,
            s.user_id
        )
        SELECT
          day_of_week,
          hour_of_day,
          country,
          AVG(total_time_spent) AS avg_time_spent
        FROM user_hourly
        GROUP BY 
          day_of_week,
          hour_of_day,
          country
        ORDER BY 
          day_of_week,
          hour_of_day,
          country;
        """
        return self._run_query(query)

    def get_avg_time_spent_per_level(self) -> pd.DataFrame:
        """
        Returns average total time spent for each level (summed across attempts).
        """
        query = """
        WITH user_level_time AS (
          SELECT
            level,
            user_id,
            SUM(time_spent) AS total_time_spent
          FROM `casedreamgames.case_db.q1_table_level_end`
          GROUP BY 1, 2
        )
        SELECT
          level,
          AVG(total_time_spent) AS avg_total_time_spent
        FROM user_level_time
        GROUP BY level
        ORDER BY level;
        """
        return self._run_query(query)

    def get_avg_time_spent_on_last_failed_level(self) -> pd.DataFrame:
        """
        Returns the average time spent on the last failed level for users 
        whose final recorded event is a failure.
        """
        query = """
        WITH final_attempt AS (
          SELECT
            user_id,
            level,
            time_spent,
            status,
            event_time,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS rn
          FROM `casedreamgames.case_db.q1_table_level_end`
        ),
        last_failed AS (
          SELECT 
            user_id,
            level,
            time_spent
          FROM final_attempt
          WHERE rn = 1
            AND status = 'fail'
        )
        SELECT 
          level,
          COUNT(*) AS num_users,
          AVG(time_spent) AS avg_time_spent_on_last_failed_level
        FROM last_failed
        GROUP BY level
        ORDER BY level;
        """
        return self._run_query(query)

    def get_revenue_by_time_spent(self) -> pd.DataFrame:
        """
        Returns daily revenue per unit of total session time:
        sum(daily_revenue) / sum(daily_time_spent).
        """
        query = """
        WITH daily_sess AS (
          SELECT
            DATE(event_time) AS date,
            user_id,
            SUM(time_spent) AS daily_time_spent
          FROM `casedreamgames.case_db.q1_table_session`
          GROUP BY date, user_id
        ),
        daily_rev AS (
          SELECT
            DATE(event_time) AS date,
            user_id,
            SUM(CAST(revenue AS FLOAT64)) AS daily_revenue
          FROM `casedreamgames.case_db.q1_table_revenue`
          GROUP BY date, user_id
        )
        SELECT
          ds.date,
          SAFE_DIVIDE(SUM(ds.daily_revenue), SUM(ds.daily_time_spent)) AS revenue_per_time_spent
        FROM (
          SELECT
            s.date,
            s.user_id,
            COALESCE(r.daily_revenue, 0) AS daily_revenue,
            s.daily_time_spent
          FROM daily_sess AS s
          LEFT JOIN daily_rev AS r
            ON s.date = r.date
           AND s.user_id = r.user_id
        ) ds
        GROUP BY ds.date
        ORDER BY ds.date;
        """
        return self._run_query(query)
    
    def get_number_of_people_passing_level(self) -> pd.DataFrame:
        query = """
        SELECT level,
              COUNT(DISTINCT user_id) AS num_people_passed
        FROM `casedreamgames.case_db.q1_table_level_end`
        WHERE status = 'win'
        GROUP BY level
        ORDER BY level;
        """
        return self._run_query(query)
    
    def get_avg_time_spent_by_groups(self) -> pd.DataFrame:
        query =  """
        WITH daily_user_session AS (
          SELECT
            DATE(s.event_time) AS date,
            i.network,
            i.country,
            s.platform,
            s.user_id,
            SUM(s.time_spent) AS daily_session_time
          FROM `casedreamgames.case_db.q1_table_session` s
          JOIN `casedreamgames.case_db.q1_table_install` i
            ON s.user_id = i.user_id
          GROUP BY 1,2,3,4,5
        )

        SELECT
          date,
          network,
          country,
          platform,
          COUNT(DISTINCT user_id) AS user_count,
          AVG(daily_session_time) AS avg_session_time_per_user
        FROM daily_user_session
        GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4;
        """
        return self._run_query(query)

    # =========================================================================
    #  6) OTHER (e.g., Conversion Rate, Levels)
    # =========================================================================
    def get_user_count_of_groups(self) -> pd.DataFrame:
        """
        Returns user counts grouped by (platform, network, country).
        """
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

    def get_conversion_rate_by_groups(self) -> pd.DataFrame:
        """
        Returns (#payers / total_users) for each (platform, network, country).
        """
        query = """
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
