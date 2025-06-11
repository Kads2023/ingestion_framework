import json
import os
from jinja2 import Environment, BaseLoader, Template
import importlib.resources as pkg_resources
from edp_automation import resources
from edp_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory


class ResourceTemplateLoader(BaseLoader):
    def __init__(self, package):
        self.package = package

    def get_source(self, environment, template):
        try:
            with pkg_resources.files(self.package).joinpath(template).open("r", encoding="utf-8") as f:
                source = f.read()
        except FileNotFoundError:
            raise TemplateNotFound(template)
        return source, None, lambda: True


def generate_artifacts(config: dict):
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

    # Use resource loader if no custom template path is provided
    custom_template_path = config.get("template_location")
    ddl_template_name = config.get("ddl_template_name", "ddl.sql.j2")
    config_template_name = config.get("config_template_name", "config.yaml.j2")
    workflow_template_name = config.get("workflow_template_name", "workflow.yaml.j2")

    if custom_template_path:
        env = Environment(loader=FileSystemLoader(custom_template_path))
    else:
        # Use internal templates packaged within the wheel
        env = Environment(loader=ResourceTemplateLoader(resources.templates))

    ddl_template = env.get_template(ddl_template_name)
    config_template = env.get_template(config_template_name)
    workflow_template = env.get_template(workflow_template_name)

    # Get mapper
    mapper = DataTypeMapperFactory.get_mapper(config["source_system_type"].lower())

    # Group columns by table
    table_columns = {}
    for entry in schema_data:
        table = entry['table_name'].upper()
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

    data_source_system_name = config["data_source_system_name"]

    # Render and write files
    for table_name, columns in table_columns.items():
        context = {
            'table_name': table_name,
            'columns': columns,
            'data_source_system_name': data_source_system_name,
        }

        ddl_output = ddl_template.render(context)
        config_output = config_template.render(context)
        workflow_output = workflow_template.render(context)

        table_dir = os.path.join(output_dir, table_name.lower())
        os.makedirs(table_dir, exist_ok=True)

        with open(os.path.join(f"{table_dir}", f"{table_name.upper()}.sql"), "w") as ddl_file:
            ddl_file.write(ddl_output + '\n')

        with open(os.path.join(f"{table_dir}", f"{table_name.lower()}.yaml"), "w") as config_file:
            config_file.write(config_output + '\n')

        with open(
                os.path.join(
                    f"{table_dir}",
                    f"edp_bronze_{data_source_system_name.lower()}_{table_name.lower()}.yaml"),
                "w"
        ) as workflow_yaml_file:
            workflow_yaml_file.write(workflow_output.strip() + '\n')

        print(
            f"Generated: {table_name}.sql, "
            f"{table_name.lower()}.yaml and "
            f"edp_bronze_{data_source_system_name.lower()}_{table_name.lower()}.yaml"
        )
