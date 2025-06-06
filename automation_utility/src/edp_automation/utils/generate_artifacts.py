# project/main_script.py

import argparse
import json
import os
from edp_automation.utils.generate_schema_file_from_source import generate_schema_file
from edp_automation.utils.generate_ddl_and_config_from_schema import generate_config_and_ddl


def parse_args():
    parser = argparse.ArgumentParser(description="Generate schema, DDL, and config from source DB")
    parser.add_argument("--tables", required=True, help="Comma-separated list of table names")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    return parser.parse_args()


def main():
    args = parse_args()

    # Read config file
    with open(args.config, "r") as f:
        config = json.load(f)

    table_names = [t.strip() for t in args.tables.split(",")]

    print("\nStep 1: Extracting schema from source DB...")
    generate_schema_file(config, table_names)

    print("\nStep 2: Generating config and DDL files from schema...")
    generate_config_and_ddl(config)


if __name__ == "__main__":
    main()
