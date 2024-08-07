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


DROP PROCEDURE IF EXISTS prod.tactical_articles_card_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_articles_card_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_clicks_months_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_clicks_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_pageviews_card_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_pageviews_card_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_userneeds_clicks_chart_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_userneeds_clicks_chart_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_userneeds_pageviews_chart_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_userneeds_pageviews_chart_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_category_clicks_chart_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_category_clicks_chart_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_category_pageviews_chart_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_category_pageviews_chart_ytd_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_articles_table_month_dbt_17;
DROP PROCEDURE IF EXISTS prod.tactical_articles_table_ytd_dbt_17;


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

    
import subprocess
import os
import yaml
import mysql.connector
from mysql.connector import Error

# Function to run DBT commands
def run_dbt_command(procedure_file):
    print('procedure_file', procedure_file)
    # Set the path to your DBT project directory
    dbt_project_path = r'E:\Kasper_Sindri_Media'

       # Set the DBT command to run the specified procedure
    dbt_command = f'dbt run --models {procedure_file}'
    # dbt_command = f'dbt compile'
    print('dbt_command:', dbt_command)

    # Change the working directory to your DBT project
    os.chdir(dbt_project_path)

    # Run the DBT command
    try:
        result = subprocess.run(dbt_command, shell=True, capture_output=True, text=True, check=True)
        print(f"STDOUT for {procedure_file}:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"STDERR for {procedure_file}:\n", e.stderr)
        print(f"Error: DBT command for {procedure_file} failed with return code {e.returncode}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Function to get SQL files from the target directory
def get_sql_files(directory):
    sql_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql"):
                sql_files.append(os.path.join(root, file))
    return sql_files

# Function to read SQL file content
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Function to create a connection to MySQL
def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='51.158.56.32',
            port=1564,
            user='Site',
            password="515B]_nP0;<|=pJOh35I",
            database='prod'
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

# Function to execute a query in MySQL
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def drop_procedure_if_exists(connection, procedure_name):
    drop_query = f"DROP PROCEDURE IF EXISTS {procedure_name};"
    execute_query(connection, drop_query)


# Function to run procedures from YAML file and create them in MySQL
def run_procedures_from_yaml():
    with open('siteDirectory.yml', 'r') as yaml_file:
        procedures = yaml.safe_load(yaml_file)
        print('procedures', procedures)
        

    if procedures and 'procedures' in procedures:
        connection = create_connection()
        for procedure_dict in procedures['procedures']:
            for procedure_name, procedure_file in procedure_dict.items():
                run_dbt_command(procedure_file)
                print('procedure_file', procedure_file)
                # Check if the procedure exists and drop it
                # drop_procedure_if_exists(connection, procedure_name)

    # Path to the DBT target directory
    # target_directory = r'E:\Kasper_Sindri_Media\target\compiled\site_dev\models\sindri_media_sites'
    # sql_files = get_sql_files(target_directory)

    # sql_scripts = [read_sql_file(file) for file in sql_files]

    # # Execute each SQL script
    # for script in sql_scripts:
    #     execute_query(connection, script)

# Function to update queries in MySQL with site ID
def update_queries_with_site_id(connection, site_id):
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
    # execute_query(connection, update_query)

if __name__ == '__main__':
    site_id = 15 
    run_procedures_from_yaml()

    connection = create_connection()
    # if connection:
    #     update_queries_with_site_id(connection, site_id)
