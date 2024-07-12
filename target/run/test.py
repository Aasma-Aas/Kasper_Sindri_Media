from jinja2 import Template
import yaml

# Specify the path to your SQL template file
sql_template_path = r'E:\DBT\DBT_Transformation\Kasper_Sindri_Media\models\example.sql'

# Specify the path to your YAML configuration file
yaml_file_path = r'E:\DBT\DBT_Transformation\Kasper_Sindri_Media\example.yml'

# Load SQL template from file
with open(sql_template_path, 'r') as sql_file:
    template_str = sql_file.read()

# Open the YAML file and load configuration
with open(yaml_file_path, 'r') as yamlfile:
    config_variables = yaml.safe_load(yamlfile)

# Create a Jinja2 template from the loaded SQL template string
template = Template(template_str)

# Render the template with variables from the YAML file
rendered_sql = template.render(
    site_id=config_variables['site_id'],
    event_action=config_variables['event_action']
)

# Print or save the rendered SQL query
print(rendered_sql)

# Save the rendered SQL to a file
output_file_path = r'E:\Kasper_Sindri_Media\target\compiled\output.sql'
with open(output_file_path, 'w') as file:
    file.write(rendered_sql)

print(f"Rendered SQL saved to {output_file_path}")
