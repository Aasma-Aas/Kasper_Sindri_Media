import mysql.connector

# Database connection details
config = {
    # 'user': 'Site',
    # 'password': '515B]_nP0;<|=pJOh35I',
    # 'host': '51.158.56.32',
    # 'port': 1564,
    # 'database': 'prod',

    'user': 'site_dev',
    'password': "87]N_8J!IlO+Y?(DB'B$",
    'host': '195.154.196.76',
    'port': 13498,
    'database': 'prod',
        
}

# Connect to the database
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# Path to the SQL file (using raw string literal)
sql_file_path = r'E:\DBT\DBT_Transformation\models\tactical_userneeds_pageviews_chart_month_11.sql'

# Read and preprocess the SQL file (if needed)
with open(sql_file_path, 'r') as file:
    sql_script = file.read()

# Print out the script for debugging
print("SQL Script:")
print(sql_script)

# Execute the SQL statements
for statement in sql_script.split(';'):
    if statement.strip():
        try:
            cursor.execute(statement)
        except mysql.connector.Error as err:
            print(f"Error executing SQL statement: {err}")

# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()

print("SQL script executed successfully.")
