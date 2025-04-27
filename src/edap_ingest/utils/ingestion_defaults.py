from datetime import datetime
from pyspark.sql.types import *

default_error_type = "error"
default_date_format = "%Y-%m-%d"

default_column_data_type = "string"
default_derived_column = False
default_type_mapping = StringType()

type_mapping = {
    "string": StringType(),
    "timestamp": TimestampType()
}

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
