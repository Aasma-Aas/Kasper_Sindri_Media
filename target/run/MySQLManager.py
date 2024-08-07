import mysql.connector
from mysql.connector import Error

class MySQLManager:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = self.create_connection()

    def create_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4')
            print("Connection to MySQL DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occurred")

    def drop_procedure_if_exists(self, procedure_name):
        drop_query = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        self.execute_query(drop_query)

    def update_queries_with_site_id(self, site_id):
        update_query = f"""
        UPDATE stage.swagger_queries_dbt
        SET names = CASE
            WHEN id = 65 THEN 'tactical_articles_card_month_dbt_{site_id}'
            WHEN id = 66 THEN 'tactical_articles_card_ytd_dbt_{site_id}'
            WHEN id = 67 THEN 'tactical_clicks_months_dbt_{site_id}'
            WHEN id = 68 THEN 'tactical_clicks_ytd_dbt_{site_id}'
            WHEN id = 69 THEN 'tactical_pageviews_card_month_dbt_{site_id}'
            WHEN id = 70 THEN 'tactical_pageviews_card_ytd_dbt_{site_id}'
            WHEN id = 71 THEN 'tactical_userneeds_clicks_chart_month_dbt_{site_id}'
            WHEN id = 72 THEN 'tactical_userneeds_clicks_chart_ytd_dbt_{site_id}'
            WHEN id = 73 THEN 'tactical_userneeds_pageviews_chart_month_dbt_{site_id}'
            WHEN id = 74 THEN 'tactical_userneeds_pageviews_chart_ytd_dbt_{site_id}'
            WHEN id = 75 THEN 'tactical_category_clicks_chart_month_dbt_{site_id}'
            WHEN id = 76 THEN 'tactical_category_clicks_chart_ytd_dbt_{site_id}'
            WHEN id = 77 THEN 'tactical_category_pageviews_chart_month_dbt_{site_id}'
            WHEN id = 78 THEN 'tactical_category_pageviews_chart_ytd_dbt_{site_id}'
            WHEN id = 79 THEN 'tactical_articles_table_month_dbt_{site_id}'
            WHEN id = 80 THEN 'tactical_articles_table_ytd_dbt_{site_id}'
        END
        WHERE id IN (65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80);
        """
        self.execute_query(update_query)
