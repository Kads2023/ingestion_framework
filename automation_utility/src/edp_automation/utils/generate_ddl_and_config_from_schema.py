import json
import os
from jinja2 import Environment, FileSystemLoader
from edp_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory


def generate_config_and_ddl(config: dict):
    """
    Generates DDL and config YAML using Jinja2 templates.

    Args:
        config (dict): Contains schema file path, template settings, and output location.
    """
    # Load schema JSON
    schema_file_path = os.path.join(config["schema_file_location"], config["schema_file_name"])

    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema file '{schema_file_path}' not found.")

    with open(schema_file_path, "r") as f:
        schema_data = json.load(f)

    # Template settings
    template_path = config.get("template_location") or os.path.join(
        os.path.dirname(__file__), "../resources/templates"
    )
    ddl_template_name = config.get("ddl_template_name", "ddl.sql.j2")
    config_template_name = config.get("config_template_name", "config.yaml.j2")

    # Setup Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_path))
    ddl_template = env.get_template(ddl_template_name)
    config_template = env.get_template(config_template_name)

    # Get mapper
    mapper = DataTypeMapperFactory.get_mapper(source_system.lower())

    # Group columns by table
    table_columns = {}
    for entry in schema_data:
        table = entry['table_name'].upper()
        if table in table_names:
            mapped_type = mapper.map_type(entry)
            table_columns.setdefault(table, []).append({
                'name': entry['column_name'],
                'datatype': mapped_type,
                'nullable': entry.get('nullable', 'Y') == 'Y',
                'identity_column': entry.get('identity_column', 'NO') == 'YES',
                'comment': entry.get('comments', '')
            })

    # Output generation
    output_dir = config["output_location"]
    os.makedirs(output_dir, exist_ok=True)

    for table_name, table_schema in schema_data.items():
        table_dir = os.path.join(output_dir, table_name.lower())
        os.makedirs(table_dir, exist_ok=True)

        ddl = ddl_template.render(table_name=table_name, columns=table_schema["columns"])
        config_yaml = config_template.render(table_name=table_name, metadata=table_schema)

        with open(os.path.join(f"{table_dir}", f"{table_name.upper()}.sql"), "w") as ddl_file:
            ddl_file.write(ddl)

        with open(os.path.join(f"{table_dir}", f"{table_name.upper()}.yaml"), "w") as config_file:
            config_file.write(config_yaml)

        print(f"Generated files for {table_name} at {table_dir}")
