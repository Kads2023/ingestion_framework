import os
import json
from jinja2 import Environment, FileSystemLoader
from edp_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory

# Directory setup
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'resources', 'templates')
INPUT_DIR = os.path.join(BASE_DIR, 'resources', 'inputs')
OUTPUT_DIR = os.path.join(BASE_DIR, 'resources', 'output')
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

    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema file '{schema_file_path}' not found.")

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
                'identity_column': entry.get('identity_column', 'NO') == 'YES',
                'comment': entry.get('comments', '')
            })

    # Render and write files
    for table, columns in table_columns.items():
        context = {
            'table_name': table,
            'columns': columns
        }
        print(f"context --> {context}")

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


# For Oracle

# from schema_extractor.oracle_schema_extractor import OracleSchemaExtractor
#
# # Connection parameters
# dsn = "ORACLE_DSN"           # or full connection string
# user = "your_username"
# password = "your_password"
# schema_owner = "HR"
#
# # Tables to extract
# table_list = ["EMPLOYEES", "DEPARTMENTS"]
# output_file = "resources/inputs/source_schema_1.json"
#
# # Extractor invocation
# extractor = OracleSchemaExtractor(user=user, password=password, dsn=dsn, schema_owner=schema_owner)
#
# extractor.connect()
# extractor.extract_metadata(table_names=table_list, output_file=output_file)
# extractor.disconnect()

# dsn = "DRIVER={Oracle in OraClient21Home1};DBQ=your_db_host:1521/your_service;UID=your_username;PWD=your_password"
# Pass this as dsn to the class and skip user and password in the string.


# oracle_extractor = OracleSchemaExtractor(
#     user='your_user',
#     password='your_password',
#     dsn='host:port/service',
#     schema_owner='YOUR_SCHEMA'
# )
# oracle_extractor.connect()
# oracle_extractor.extract_metadata(['EMPLOYEES', 'DEPARTMENTS'])
# oracle_extractor.disconnect()

# For SQL Server
# sqlserver_extractor = SQLServerSchemaExtractor(
#     connection_string='DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;UID=user;PWD=pwd',
#     schema_owner='dbo'
# )
# sqlserver_extractor.connect()
# sqlserver_extractor.extract_metadata(['EMPLOYEES', 'DEPARTMENTS'])
# sqlserver_extractor.disconnect()
