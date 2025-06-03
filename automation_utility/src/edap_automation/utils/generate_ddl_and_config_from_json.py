import os
import json
from jinja2 import Environment, FileSystemLoader
from edap_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory

# Jinja setup
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
INPUT_DIR = os.path.join(BASE_DIR, 'inputs')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set up Jinja2 environment
env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    trim_blocks=True,     # removes newline after block tags (e.g. {% endif %})
    lstrip_blocks=True,    # strips leading spaces before block tags)
    keep_trailing_newline=True
)

ddl_template = env.get_template('ddl.sql.j2')
config_template = env.get_template('config.yaml.j2')


def generate_artifacts(source_system: str, schema_filename: str, table_list_str: str):
    table_names = [t.strip().upper() for t in table_list_str.split(',')]
    schema_file_path = os.path.join(INPUT_DIR, schema_filename)

    # Load schema JSON
    with open(schema_file_path, 'r') as f:
        schema_data = json.load(f)

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
                'comment': entry.get('comments', '')
            })

    # Render and write files
    for table, columns in table_columns.items():
        context = {'table_name': table, 'columns': columns}

        ddl_output = ddl_template.render(context)
        config_output = config_template.render(context)

        with open(os.path.join(OUTPUT_DIR, f"{table}.sql"), 'w') as ddl_file:
            ddl_file.write(ddl_output.strip() + '\n')

        with open(os.path.join(OUTPUT_DIR, f"{table}.yaml"), 'w') as yaml_file:
            yaml_file.write(config_output.strip() + '\n')

        print(f"✅ Generated: {table}.sql and {table}.yaml")




generate_artifacts("oracle", "oracle_schema.json", "EMPLOYEES")

# Optional CLI entrypoint
# if __name__ == "__main__":
#     import sys
#
#     if len(sys.argv) != 4:
#         print("Usage: python generate.py <source_system> <schema_file.json> <comma_separated_table_names>")
#         sys.exit(1)
#
#     source_system = sys.argv[1]
#     schema_filename = sys.argv[2]
#     table_list_str = sys.argv[3]
#
#     generate_artifacts(source_system, schema_filename, table_list_str)
