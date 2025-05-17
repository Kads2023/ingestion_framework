from pyspark.sql import DataFrame as Spark_Dataframe
import pandas as pd

default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"

data_type_defaults_old = {
    "str": {"default_value": "", "type_name": "string"},
    "int": {"default_value": 0, "type_name": "integer"},
    "list": {"default_value": [], "type_name": "list"},
    "dict": {"default_value": {}, "type_name": "dictionary"},
    "bool": {"default_value": False, "type_name": "boolean"}
}

data_type_defaults = {
    "str": {"type": str, "type_name": "string"},
    "int": {"type": int, "type_name": "integer"},
    "float": {"type": float},
    "bool": {"type": bool, "type_name": "boolean"},
    "list": {"type": list},
    "dict": {"type": dict, "type_name": "dictionary"},
    "tuple": {"type": tuple},
    "set": {"type": set},
    "none": {"type": type(None)},
    "pandas_dataframe": {"type": pd.DataFrame},
    "spark_dataframe": {"type": Spark_Dataframe},
}

default_true_values_list = ["True", "true", "Yes", "yes", "1"]
