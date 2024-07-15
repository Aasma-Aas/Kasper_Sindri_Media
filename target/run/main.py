from jinja2 import Template
import yaml, os

class SQLRenderer:
    def __init__(self, siteDirectory_yaml_path, yaml_config):
        self.yaml_config = yaml_config
        self.siteDirectory_yaml_path = siteDirectory_yaml_path
        self.config_variables = self.load_yaml(self.yaml_config)
        self.site_config = self.load_yaml(self.siteDirectory_yaml_path)
        self.folder_path = self.get_folder_path()

    def load_yaml(self, path):
        with open(path, 'r') as file:
            return yaml.safe_load(file)

    
    # print(event_action)
    def get_folder_path(self):
        event_action_list = ['Next Click']
        for rule in self.site_config['rules']:
            print(rule)
            if rule['event_action'] in event_action_list:
                return rule['folder_path']
        raise ValueError(f"No folder path found for event_action in {event_action_list}")

    def render_sql(self):
        procedures = self.site_config.get('procedures', [])
        for proc in procedures:
            for key, value in proc.items():
                sql_template_path = os.path.join(self.folder_path, value)
                if not os.path.isfile(sql_template_path):
                    raise FileNotFoundError(f"SQL template file not found: {sql_template_path}")
                
                with open(sql_template_path, 'r') as sql_file:
                    template_str = sql_file.read()

                template = Template(template_str)
                for i in self.config_variables['sites']:
                    print(i)
                    rendered_sql = template.render(
                        site_id=i['site_id'],
                        event_action=i['event_action'],
                        date_query=i['date_query'],
                        cards_count=i['cards_count'],
                        count=i['count'],
                        tendency_cards_label=i['tendency_cards'][0],
                        tendency_cards_hint=i['tendency_cards'][1],
                        tendency_chart_var_1=i['tendency_chart_var_1'],
                        tendency_chart_var_2=i['tendency_chart_var_2'],
                        tendency_chart_var_3=i['tendency_chart_var_3']
                    )
                    output_file_name = f"{value}_output.sql"
                    path = r"E:\Kasper_Sindri_Media\target\compiled" 
                    # output_file_path = os.path.join(self.folder_path, output_file_name)
                    output_file_path = os.path.join(path, output_file_name)
                    with open(output_file_path, 'w') as file:
                        file.write(rendered_sql)
                    print(f"Rendered SQL saved to {output_file_path}")

if __name__ == "__main__":
    
    siteDirectory_yaml_path = r'E:\Kasper_Sindri_Media\siteDirectory.yml'
    yaml_config = r'E:\Kasper_Sindri_Media\config-variables.yml'
    
    sql_renderer = SQLRenderer(siteDirectory_yaml_path, yaml_config)
    sql_renderer.render_sql()
