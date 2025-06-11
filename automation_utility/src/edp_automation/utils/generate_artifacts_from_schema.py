import json
import os
from jinja2 import Environment, BaseLoader, TemplateNotFound, FileSystemLoader
import importlib.resources as pkg_resources
import edp_automation.resources.templates as templates_dir
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
    Generates DDL, config YAML, workflow YAML, and optionally other artifacts using Jinja2 templates.

    Args:
        config (dict): Contains schema file path, template settings, and output location.
    """
    schema_file_path = os.path.join(config["schema_file_location"], config["schema_file_name"])
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema file '{schema_file_path}' not found.")

    with open(schema_file_path, "r") as f:
        schema_data = json.load(f)

    # Use resource loader if no custom template path is provided
    custom_template_path = config.get("template_location")

    if custom_template_path:
        env = Environment(
            loader=FileSystemLoader(custom_template_path),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True
        )
    else:
        # Use internal templates packaged within the wheel
        env = Environment(
            loader=ResourceTemplateLoader(templates_dir),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True
        )

    # Optional additional templates
    extra_templates = config.get("extra_templates", [])  # List of dicts with template_name and output_file_name

    artifacts_to_generate = config.get("artifacts_to_generate", "all").lower()
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

    output_dir = config["output_location"]
    os.makedirs(output_dir, exist_ok=True)

    data_source_system_name = config["data_source_system_name"]

    for table_name, columns in table_columns.items():
        context = {
            'table_name': table_name,
            'columns': columns,
            'data_source_system_name': data_source_system_name,
        }

        table_dir = os.path.join(output_dir, table_name.lower())
        os.makedirs(table_dir, exist_ok=True)

        # Standard files
        if artifacts_to_generate in ("ddl", "all"):
            ddl_template = env.get_template(config.get("ddl_template_name", "ddl.sql.j2"))
            ddl_file_name = config.get("ddl_file_name", f"{table_name.upper()}.sql").format(
                    data_source_system_name=data_source_system_name.lower(),
                    table_name=table_name.lower()
                )
            ddl_output = ddl_template.render(context)
            with open(os.path.join(f"{table_dir}", ddl_file_name), "w") as f:
                f.write(ddl_output + '\n')

        if artifacts_to_generate in ("config", "all"):
            config_template = env.get_template(config.get("config_template_name", "config.yaml.j2"))
            config_file_name = config.get("config_file_name", f"{table_name.lower()}.yaml").format(
                    data_source_system_name=data_source_system_name.lower(),
                    table_name=table_name.lower()
                )
            config_output = config_template.render(context)
            with open(os.path.join(f"{table_dir}", config_file_name), "w") as f:
                f.write(config_output + '\n')

        if artifacts_to_generate in ("workflow", "all"):
            workflow_template = env.get_template(config.get("workflow_template_name", "workflow.yaml.j2"))
            workflow_name = f"edp_bronze_{data_source_system_name.lower()}_{table_name.lower()}.yaml"
            workflow_file_name = config.get("config_file_name", workflow_name).format(
                    data_source_system_name=data_source_system_name.lower(),
                    table_name=table_name.lower()
                )
            workflow_output = workflow_template.render(context)
            with open(os.path.join(f"{table_dir}", workflow_file_name), "w") as f:
                f.write(workflow_output.strip() + '\n')

        # Extra templates with dynamic output filenames
        for extra in extra_templates:
            try:
                template = env.get_template(extra["template_name"])
                output = template.render(context)

                # Allow placeholders in output_file_name, e.g. edp_{data_source_system_name}_{table_name}.txt
                output_file_name = extra["output_file_name"].format(
                    data_source_system_name=data_source_system_name.lower(),
                    table_name=table_name.lower()
                )

                output_file_path = os.path.join(f"{table_dir}", output_file_name)
                with open(output_file_path, "w") as f:
                    f.write(output + '\n')
            except Exception as e:
                print(f"Error processing extra template {extra.get('template_name')}: {e}")

        print(f"Generated files for {table_name}")
