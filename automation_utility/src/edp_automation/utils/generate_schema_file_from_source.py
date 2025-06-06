# edp_automation/utils/generate_schema_file_from_source.py

import json
import getpass
from edp_automation.schema_extractor.schema_extractor_factory import SchemaExtractorFactory


def generate_schema_file(config: dict, table_names: list):
    """
    Extracts schema from source DB and writes to a JSON file.
    Prompts the user for DB password at runtime.

    Args:
        config (dict): Contains DB config and output file details (except password).
        table_names (list): List of table names.
    """
    # Prompt for password securely
    db_password = getpass.getpass(prompt="Enter database password: ")

    # Create extractor
    extractor = SchemaExtractorFactory.get_extractor(
        source_system=config["source_system_type"],
        user=config["db_user"],
        password=db_password,
        dsn=config["data_source_name"],
        schema_owner=config["schema_owner"]
    )

    # Extract schema metadata for each table
    schema_data = {}
    for table in table_names:
        schema_data[table] = extractor.extract_table_metadata(table)

    # Write to schema JSON file
    schema_file_path = f"{config['schema_file_location']}/{config['schema_file_name']}"
    with open(schema_file_path, "w") as f:
        json.dump(schema_data, f, indent=4)

    print(f"Schema file written to {schema_file_path}")
