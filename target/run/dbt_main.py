# import subprocess
# import os

# def run_dbt_command():
#     # Set the path to your DBT project directory
#     dbt_project_path = r'E:\Kasper_Sindri_Media'

#     # Set the DBT command you want to run
#     dbt_command = 'dbt run --models example.Tactical_clicks_months_12'

#     # Change the working directory to your DBT project
#     os.chdir(dbt_project_path)

#     # Run the DBT command
#     result = subprocess.run(dbt_command, shell=True, capture_output=True, text=True)

#     # Print the command output
#     print("STDOUT:\n", result.stdout)
#     print("STDERR:\n", result.stderr)

#     # Check for errors
#     if result.returncode != 0:
#         print("Error: DBT command failed")
#     else:
#         print("DBT command executed successfully")

# if __name__ == '__main__':
#     run_dbt_command()


import subprocess
import os
import yaml

def run_dbt_command(procedure_file):
    # Set the path to your DBT project directory
    dbt_project_path = r'E:\Kasper_Sindri_Media'

    # Set the DBT command to run the specified procedure
    dbt_command = f'dbt run --models {procedure_file}'

    # Change the working directory to your DBT project
    os.chdir(dbt_project_path)

    # Run the DBT command
    result = subprocess.run(dbt_command, shell=True, capture_output=True, text=True)

    # Print the command output
    print(f"STDOUT for {procedure_file}:\n", result.stdout)
    print(f"STDERR for {procedure_file}:\n", result.stderr)

    # Check for errors
    if result.returncode != 0:
        print(f"Error: DBT command for {procedure_file} failed")
    else:
        print(f"DBT command for {procedure_file} executed successfully")

def run_procedures_from_yaml():
    # Load procedures from YAML
    with open('siteDirectory.yml', 'r') as yaml_file:
        procedures = yaml.safe_load(yaml_file)

    if procedures and 'procedures' in procedures:
        for procedure_dict in procedures['procedures']:
            for procedure_name, procedure_file in procedure_dict.items():
                run_dbt_command(procedure_file)

if __name__ == '__main__':
    run_procedures_from_yaml()
