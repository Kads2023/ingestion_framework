"""
Module containing default configurations, constants, and mappings
for ingestion processes.

This module defines standard default values for input parameters,
type mappings for Spark data types, and other reusable configurations
to be used across the ingestion framework.

Constants:
    default_date_format (str): Default date format ("%Y-%m-%d").
    default_column_data_type (str): Default data type for columns ("string").
    default_derived_column (bool): Default flag indicating whether a column is derived (False).
    default_type_mapping (DataType): Default Spark data type mapping (StringType).

Mappings:
    type_mapping (dict): Maps string representations of types to corresponding Spark SQL types.

Input Parameters:
    input_params_keys (list): List of supported input parameter keys.
    mandatory_input_params (list): List of mandatory input parameters required for ingestion.
    default_values_for_input_params (dict): Default values assigned to optional input parameters.
    input_params_to_be_converted_to_bool (list): List of input parameters that should be typecast to boolean.

Dependencies:
    - datetime from Python Standard Library
    - pyspark.sql.types for Spark SQL type definitions
"""

from datetime import datetime

default_date_format = "%Y-%m-%d"

default_column_data_type = "string"
default_derived_column = False

input_params_keys = [
    "env",
    "ingest_type",
    "run_date",
    "common_config_file_location",
    "table_config_file_location",
    "dry_run"
]

mandatory_input_params = [
    "common_config_file_location",
    "table_config_file_location"
]

default_values_for_input_params = {
    "env": "dev",
    "ingest_type": "csv",
    "ingest_layer": "bronze",
    "run_date": datetime.utcnow().strftime(default_date_format),
    "dry_run": "False"
}

input_params_to_be_converted_to_bool = [
    "dry_run"
]
