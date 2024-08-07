import subprocess
import os
import yaml
import shutil

class DBTManager:
    def __init__(self, dbt_project_path, site_folder):
        self.dbt_project_path = dbt_project_path
        self.site_folder = site_folder

    def copy_dbt_project_file(self, site_id):
        src_file = f'E:\\Kasper_Sindri_Media\\dbt_project_{site_id}.yml'
        print('src_file', src_file)
        dst_file = 'E:\\Kasper_Sindri_Media\\dbt_project.yml'
        try:
            shutil.copyfile(src_file, dst_file)
            print(f"Copied {src_file} to {dst_file}")
        except Exception as e:
            print(f"Failed to copy {src_file} to {dst_file}: {e}")

    def run_dbt_command(self, procedure_file):
        print('procedure_file', procedure_file)
        dbt_command = f'dbt run --models {procedure_file}'
        print('dbt_command:', dbt_command)
        os.chdir(self.dbt_project_path)
        target_dir = os.path.join(self.dbt_project_path, 'target', 'compiled', self.site_folder)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        try:
            result = subprocess.run(dbt_command, shell=True, capture_output=True, text=True, check=True)
            print(f"STDOUT for {procedure_file}:\n", result.stdout)
            self.move_compiled_sql_files(target_dir)
        except subprocess.CalledProcessError as e:
            print(f"STDERR for {procedure_file}:\n", e.stderr)
            print(f"Error: DBT command for {procedure_file} failed with return code {e.returncode}")
            self.move_compiled_sql_files(target_dir)
        except Exception as e:
            print(f"Unexpected error: {e}")

    def move_compiled_sql_files(self, target_dir):
        compiled_output_dir = os.path.join(self.dbt_project_path, 'target', 'compiled', 'site_dev', 'models', 'sindri_media_sites')
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

    # def get_sql_files(self, directory):
    #     sql_files = []
    #     for root, dirs, files in os.walk(directory):
    #         for file in files:
    #             if file.endswith(".sql"):
    #                 sql_files.append(os.path.join(root, file))
    #     return sql_files

    # def read_sql_file(self, file_path):
    #     with open(file_path, 'r', encoding='utf-8') as file:
    #         return file.read()


    def get_and_read_sql_files(self, directory):
        sql_files_contents = {}
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".sql"):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_files_contents[file_path] = f.read()
        return sql_files_contents


    def run_procedures_from_yaml(self, yaml_file_path, mysql_manager):
        with open(yaml_file_path, 'r') as yaml_file:
            procedures = yaml.safe_load(yaml_file)
            print('procedures', procedures)
        
        if procedures and 'procedures' in procedures:
            for procedure_dict in procedures['procedures']:
                for procedure_name, procedure_file in procedure_dict.items():
                    self.run_dbt_command(procedure_file)
                    print('procedure_file', procedure_file)
                    mysql_manager.drop_procedure_if_exists(procedure_name)

        target_directory = os.path.join(self.dbt_project_path, 'target', 'compiled', self.site_folder)
        sql_files_contents = self.get_and_read_sql_files(target_directory)

        for file_path, content in sql_files_contents.items():
            print(f"File: {file_path}")
            print(f"Content: {content}")
            # Execute the content as SQL script
            mysql_manager.execute_query(content)


        # for script in sql_scripts:
        #     mysql_manager.execute_query(script)
