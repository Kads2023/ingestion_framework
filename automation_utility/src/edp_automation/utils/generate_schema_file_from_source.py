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
    extractor = None
    try:
        # Prompt for password securely
        db_password = getpass.getpass(prompt="Enter database password: ")

        # Create extractor
        extractor = SchemaExtractorFactory.get_schema_extractor(
            source_system_type=config["source_system_type"],
            user=config["db_user"],
            password=db_password,
            dsn=config["dsn"],
            schema_name=config["schema_name"]
        )

        # Write to schema JSON file
        schema_file_path = f"{config['schema_file_location']}/{config['schema_file_name']}"
        print(f"schema_file_path --> {schema_file_path}")

        extractor.connect()
        extractor.extract_metadata(table_names, schema_file_path)
    except Exception as ex:
        print(f"Raised an Exception, ({ex})")
    finally:
        try:
            if extractor:
                extractor.disconnect()
        except Exception as e:
            print(f"Raised an Exception while closing the extractor, ({e})")
