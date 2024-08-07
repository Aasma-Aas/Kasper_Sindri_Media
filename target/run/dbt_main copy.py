from DBTManager import DBTManager
from MySQLManager import MySQLManager

if __name__ == '__main__':
    site_folder = 'site15folder'
    yaml_file_path = 'siteDirectory.yml'
    dbt_project_path = r'E:\Kasper_Sindri_Media'

    # Create instances of the managers
    dbt_manager = DBTManager(dbt_project_path, site_folder)
    mysql_manager = MySQLManager(
        host='51.158.56.32',
        port=1564,
        user='Site',
        password="515B]_nP0;<|=pJOh35I",
        database='prod'
    )

    # Run procedures from YAML and update queries with site ID
    dbt_manager.run_procedures_from_yaml(yaml_file_path, mysql_manager)
    mysql_manager.update_queries_with_site_id(15)
