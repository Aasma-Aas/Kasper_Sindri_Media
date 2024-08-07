import subprocess
import os
import shutil
import yaml
import mysql.connector
from mysql.connector import Error
import csv
import yaml

def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='51.158.56.32',
            port=1564,
            user='Site',
            password="515B]_nP0;<|=pJOh35I",
            database='prod',
            charset='utf8mb4'
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def run_dbt_command(procedure_file, site_folder):
    dbt_project_path = r'E:\Kasper_Sindri_Media'
    dbt_command = f'dbt run --models {procedure_file}'
    os.chdir(dbt_project_path)
    target_dir = os.path.join(dbt_project_path, 'target', 'compiled', site_folder)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    try:
        result = subprocess.run(dbt_command, shell=True, capture_output=True, text=True, check=True)
        print(f"STDOUT for {procedure_file}:\n", result.stdout)
        move_compiled_sql_files(dbt_project_path, target_dir)
    except subprocess.CalledProcessError as e:
        print(f"STDERR for {procedure_file}:\n", e.stderr)
        print(f"Error: DBT command for {procedure_file} failed with return code {e.returncode}")
        move_compiled_sql_files(dbt_project_path, target_dir)
    except Exception as e:
        print(f"Unexpected error: {e}")

# Function to move compiled SQL files
def move_compiled_sql_files(dbt_project_path, target_dir):
    compiled_output_dir = os.path.join(dbt_project_path, 'target', 'compiled', 'site_dev', 'models', 'sindri_media_sites')
    print(f'Compiled output directory: {compiled_output_dir}')
    if not os.path.exists(compiled_output_dir):
        print(f"Compiled output directory does not exist: {compiled_output_dir}")
        return
    for root, _, files in os.walk(compiled_output_dir):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                target_path = os.path.join(target_dir, file)
                print(f"Moving {file_path} to {target_path}")
                if os.path.exists(target_path):
                    os.remove(target_path)
                os.rename(file_path, target_path)
                print(f"Moved {file_path} to {target_path}")

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
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

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
def run_procedures_from_yaml(site_folder):
    with open('siteDirectory.yml', 'r') as yaml_file:
        procedures = yaml.safe_load(yaml_file)
        print('procedures', procedures)
    if procedures and 'procedures' in procedures:
        connection = create_connection()
        for procedure_dict in procedures['procedures']:
            for procedure_name, procedure_file in procedure_dict.items():
                run_dbt_command(procedure_file, site_folder)
                print('procedure_name', procedure_name)
                drop_procedure_if_exists(connection, procedure_name)


    # target_directory = os.path.join(r'E:\Kasper_Sindri_Media\target\compiled', site_folder)
    # sql_files = get_sql_files(target_directory)
    # sql_scripts = [read_sql_file(file) for file in sql_files]
    # for script in sql_scripts:
    #     execute_query(connection, script)

# Function to update queries in MySQL with site ID
def update_queries_with_site_id(connection, site_id):
    query_dic = {
        65: f'tactical_articles_card_month_dbt_{site_id}',
        66: f'tactical_articles_card_ytd_dbt_{site_id}',
        67: f'tactical_clicks_months_dbt_{site_id}',
        68: f'tactical_clicks_ytd_dbt_{site_id}',
        69: f'tactical_pageviews_card_month_dbt_{site_id}',
        70: f'tactical_pageviews_card_ytd_dbt_{site_id}',
        71: f'tactical_userneeds_clicks_chart_month_dbt_{site_id}',
        72: f'tactical_userneeds_clicks_chart_ytd_dbt_{site_id}',
        73: f'tactical_userneeds_pageviews_chart_month_dbt_{site_id}',
        74: f'tactical_userneeds_pageviews_chart_ytd_dbt_{site_id}',
        75: f'tactical_category_clicks_chart_month_dbt_{site_id}',
        76: f'tactical_category_clicks_chart_ytd_dbt_{site_id}',
        77: f'tactical_category_pageviews_chart_month_dbt_{site_id}',
        78: f'tactical_category_pageviews_chart_ytd_dbt_{site_id}',
        79: f'tactical_articles_table_month_dbt_{site_id}',
        80: f'tactical_articles_table_ytd_dbt_{site_id}'
    }
    update_query = "UPDATE stage.swagger_queries_dbt SET names = CASE\n"
    for id, name in query_dic.items():
        update_query += f"    WHEN id = {id} THEN '{name}'\n"
    update_query += "END\n"
    update_query += "WHERE id IN (" + ", ".join(map(str, query_dic.keys())) + ");"
    execute_query(connection, update_query)

# Function to update the vars section in dbt_project.yml
def update_vars_in_dbt_project(csv_file_path, dbt_project_yml_path):
    # Read the CSV file
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        csv_data = [row for row in reader]

    # Load the dbt_project.yml file
    with open(dbt_project_yml_path, 'r') as ymlfile:
        dbt_project = yaml.safe_load(ymlfile)

    # Initialize 'vars' if not present
    if 'vars' not in dbt_project:
        dbt_project['vars'] = {}

    # Update the vars section with data from the CSV
    for row in csv_data:
        for key, value in row.items():
            # Clean up the value to avoid extra quotes and escape sequences
            cleaned_value = value.replace('"""', '"').replace("\'", "'").strip()
            dbt_project['vars'][key] = cleaned_value

    # Save the updated dbt_project.yml file
    with open(dbt_project_yml_path, 'w', encoding='utf-8') as ymlfile:
        yaml.safe_dump(dbt_project, ymlfile, allow_unicode=True)

if __name__ == '__main__':
    site_folder = 'site15folder'
    site_id = 15
    csv_file_path = 'E:\Kasper_Sindri_Media\site_dir.csv'
    dbt_project_yml_path = 'E:\Kasper_Sindri_Media\dbt_project.yml'
    update_vars_in_dbt_project(csv_file_path, dbt_project_yml_path)
    run_procedures_from_yaml(site_folder)
    connection = create_connection()
    if connection:
        update_queries_with_site_id(connection, site_id)


