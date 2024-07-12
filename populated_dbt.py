import yaml
import argparse
import os
import shutil

def read_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def update_dbt_project(client_yaml, dbt_project_path):
    with open(dbt_project_path, 'r') as file:
        dbt_project = yaml.safe_load(file)

    dbt_project['vars'] = client_yaml['vars']

    with open(dbt_project_path, 'w') as file:
        yaml.safe_dump(dbt_project, file)

def run_dbt_compile(site_id=None, all_clients=False):
    if all_clients:
        # Run dbt compile for all clients
        clients_dir = 'clients'
        for client_file in os.listdir(clients_dir):
            if client_file.endswith('.yml'):
                client_yaml = read_yaml(os.path.join(clients_dir, client_file))
                update_dbt_project(client_yaml, 'dbt_project.yml')
                os.system('dbt compile')
                copy_to_client_directory(client_yaml['vars']['site_id'])
    elif site_id:
        # Run dbt compile for a specific client
        client_file = f'clients/{site_id}.yml'
        client_yaml = read_yaml(client_file)
        update_dbt_project(client_yaml, 'dbt_project.yml')
        os.system('dbt compile')
        copy_to_client_directory(client_yaml['vars']['site_id'])

def copy_to_client_directory(site_id):
    source_dir = 'target/compiled'
    destination_dir = f'sites/{site_id}/compiled_models'
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    for item in os.listdir(source_dir):
        s = os.path.join(source_dir, item)
        d = os.path.join(destination_dir, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store_true', help='Compile for all clients')
    parser.add_argument('-id', '--site_id', type=str, help='Site ID of the client to compile')
    args = parser.parse_args()

    run_dbt_compile(site_id=args.site_id, all_clients=args.all)
