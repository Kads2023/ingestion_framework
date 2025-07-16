import enum
import inspect
import traceback

import pandas as pd
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from pyspark.sql import DataFrame as Spark_Dataframe

import pyspark.sql.functions as f

from typing import Type

from datetime import datetime
import yaml
from builtins import open
import functools
import time

from edap_common.utils.constants import *
# from edap_common.utils.log_wrapper import LogWrapper
from pyspark.sql.utils import AnalysisException


class CommonUtils:
    def __init__(self, lc):
        """
        Args:
            lc: Logger class instance
        """
        self.this_class_name = f"{type(self).__name__}"
        self.lc = lc
        # self.log_wrapper = LogWrapper(lc)
        self.spark = SparkSession.getActiveSession()

    # def log_msg(self, passed_log_string, passed_logger_type=default_log_type):
    #     self.log_wrapper.log_or_print(passed_log_string, passed_logger_type)

    def get_current_time(self, datetime_format=default_datetime_format):
        this_module = f"[{self.this_class_name}.get_current_time()] -"
        self.validate_function_param(
            this_module,
            {
                "datetime_format": {
                    "input_value": datetime_format,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        ret_time = datetime.utcnow().strftime(datetime_format)
        self.lc.logger.info(
            f"{this_module} "
            f"date_time_format --> {datetime_format}, "
            f"ret_time --> {ret_time}"
        )
        return ret_time

    def check_and_evaluate_str_to_bool(self, passed_str):
        this_module = f"[{self.this_class_name}.check_and_evaluate_str_to_bool()] -"
        self.validate_function_param(
            this_module,
            {
                "passed_str": {
                    "input_value": passed_str,
                    "data_type": "str",
                },
            },
        )
        ret_bool = False
        check_str = passed_str.lower().capitalize()
        try:
            ret_bool = bool(eval(check_str))
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            if check_str in default_true_values_list:
                ret_bool = True
        return ret_bool

    def get_date_split(self, passed_date, date_format=default_date_format):
        this_module = f"[{self.this_class_name}.get_date_split()] -"
        self.validate_function_param(
            this_module,
            {
                "passed_date": {
                    "input_value": passed_date,
                    "data_type": "str",
                    "check_empty": True,
                },
                "date_format": {
                    "input_value": date_format,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        try:
            date_object = datetime.strptime(passed_date, date_format)
            year = date_object.strftime("%Y")
            month = date_object.strftime("%m")
            day = date_object.strftime("%d")
            return year, month, day
        except Exception as e:
            self.lc.logger.error(
                f"{this_module} "
                f"passed_date --> {passed_date}, "
                f"date_format --> {date_format}, ({e})"
            )
            raise

    def check_and_set_dbutils(self, dbutils=None):
        this_module = f"[{self.this_class_name}.check_and_set_dbutils()] -"
        if dbutils is not None:
            ret_dbutils = dbutils
        else:
            spark = SparkSession.getActiveSession()
            try:
                from pyspark.dbutils import DBUtils

                ret_dbutils = DBUtils(spark)
            except ModuleNotFoundError as e:
                self.lc.logger.error(
                    f"{this_module} ModuleNotFoundError : "
                    f"error while creating dbutils object ({e})"
                )
                raise
            except Exception as e:
                self.lc.logger.error(
                    f"{this_module} Exception : "
                    f"error while creating dbutils object ({e})"
                )
                raise
        return ret_dbutils

    def read_yaml_file(self, passed_file_path):
        this_module = f"[{self.this_class_name}.read_yaml_file()] -"
        self.validate_function_param(
            this_module,
            {
                "passed_file_path": {
                    "input_value": passed_file_path,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        try:
            with open(passed_file_path, "r") as file_data:
                ret_dict = yaml.safe_load(file_data)
                return ret_dict
        except FileNotFoundError as fnf:
            self.lc.logger.error(
                f"{this_module} FileNotFoundError: YAML file not found at {passed_file_path} - {fnf}"
            )
            raise
        except yaml.YAMLError as ye:
            self.lc.logger.error(
                f"{this_module} YAMLError: Error parsing YAML file {passed_file_path} - {ye}"
            )
            raise
        except Exception as e:
            self.lc.logger.error(
                f"{this_module} Unexpected Exception: error while reading YAML file ({e})"
            )
            raise

    def read_file_as_string(self, passed_file_path):
        """
        Reads the contents of a file and returns it as a string.

        Args:
            passed_file_path (str): Path to the file.

        Returns:
            str: Content of the file.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If access to the file is denied.
            ValueError: If the file path is invalid.
            IOError: For any other I/O related errors.
        """
        this_module = f"[{self.this_class_name}.read_file_as_string()] -"
        self.validate_function_param(
            this_module,
            {
                "passed_file_path": {
                    "input_value": passed_file_path,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        try:
            with open(passed_file_path, 'r', encoding='utf-8') as file_content:
                return file_content.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"The file '{passed_file_path}' does not exist.")
        except PermissionError:
            raise PermissionError(f"Permission denied to read the file: {passed_file_path}")
        except OSError as e:
            raise IOError(f"Failed to read file '{passed_file_path}': {e}")

    def validate_function_param_old(self, passed_module, params_dict: dict):
        this_module = f"[{self.this_class_name}.validate_function_param_old()] - {passed_module} -"
        for each_param_name in params_dict.keys():
            each_param_dict = params_dict[each_param_name]
            each_param_value = each_param_dict["input_value"]
            each_param_data_type = each_param_dict["data_type"]
            each_param_check_empty = each_param_dict.get("check_empty", False)

            cur_data_type_defaults = data_type_defaults_old[each_param_data_type]
            default_value = cur_data_type_defaults["default_value"]
            type_name = cur_data_type_defaults["type_name"]
            if type(each_param_value) is not type(default_value):
                error_msg = (
                    f"{passed_module} "
                    f"{each_param_name} -- "
                    f"{each_param_value} must be "
                    f"a/an {type_name}"
                )
                self.lc.logger.error(error_msg)
                raise TypeError(error_msg)
            if each_param_check_empty:
                if each_param_value == default_value:
                    error_msg = (
                        f"{passed_module} "
                        f"{each_param_name} -- "
                        f"{each_param_value} must not be "
                        f"a/an empty {type_name} -- "
                        f"{default_value}"
                    )
                    self.lc.logger.error(error_msg)
                    raise ValueError(error_msg)

    def validate_function_param(self, passed_module, params_dict: dict, custom_type: dict[str, Type]=None):
        this_module = f"[{self.this_class_name}.validate_function_param()] - {passed_module} -"
        for each_param_name in params_dict.keys():
            each_param_dict = params_dict[each_param_name]
            each_param_value = each_param_dict["input_value"]
            each_param_data_type = each_param_dict["data_type"]
            each_param_check_empty = each_param_dict.get("check_empty", False)

            if custom_type:
                data_type_defaults.update(custom_type)

            cur_data_type_defaults = data_type_defaults.get(each_param_data_type)
            if cur_data_type_defaults is None:
                error_msg = (
                    f"{this_module} "
                    f"each_param_dict --> {each_param_dict}, "
                    f"custom_type --> {custom_type}, "
                    f"data_type_defaults --> {data_type_defaults}, "
                    f"ValueError: UNSUPPORTED TYPE: "
                    f"{each_param_data_type}"
                )
                self.lc.logger.error(error_msg)
                raise ValueError(error_msg)
            type_name = cur_data_type_defaults.get("type_name", each_param_data_type)
            expected_type = cur_data_type_defaults["type"]
            if not isinstance(each_param_value, expected_type):
                error_msg = (
                    f"{passed_module} "
                    f"{each_param_name} -- "
                    f"{each_param_value} must be "
                    f"a/an {type_name}"
                )
                self.lc.logger.error(error_msg)
                raise TypeError(error_msg)
            if each_param_check_empty:
                raise_exception = False
                if each_param_value is None:
                    raise_exception = True
                if custom_type:
                    if each_param_data_type in custom_type.keys():
                        if issubclass(expected_type, enum.Enum):
                            if not(each_param_value in expected_type.__members__):
                                raise_exception = True
                        elif (
                            inspect.isclass(each_param_value)
                            and isinstance(each_param_value, expected_type)
                            and len(each_param_value) == 0
                        ):
                            raise_exception = True
                        elif each_param_value.isvalid():
                            raise_exception = True
                else:
                    if(
                        isinstance(each_param_value, (str, list, tuple, dict, set))
                        and len(each_param_value) == 0
                    ):
                        raise_exception = True
                    if (
                        isinstance(each_param_value, pd.DataFrame)
                        and each_param_value.empty
                    ):
                        raise_exception = True
                    if (
                        isinstance(each_param_value, Spark_Dataframe)
                        and each_param_value.rdd.isRmpty()
                    ):
                        raise_exception = True
                if raise_exception:
                    error_msg = (
                        f"{passed_module} "
                        f"{each_param_name} -- "
                        f"{each_param_value} must not be "
                        f"a/an empty {type_name}"
                    )
                    self.lc.logger.error(error_msg)
                    raise ValueError(error_msg)

    def retry_on_exception(
            self,
            exceptions=(Exception,),
            max_attempts=3,
            delay_seconds=2,
            backoff_factor=1.0,
    ):
        """
        Retry decorator that retries a function if specified exceptions occur.

        Args:
            exceptions (tuple): Exception types to catch and retry on.
            max_attempts (int): Maximum number of attempts before giving up.
            delay_seconds (int): Initial delay between retries in seconds.
            backoff_factor (float): Multiplier to increase delay each retry.

        Returns:
            Decorator that applies retry logic.
        """

        def decorator_retry(func):
            @functools.wraps(func)
            def wrapper_retry(*args, **kwargs):
                attempts = 0
                delay = delay_seconds
                while attempts < max_attempts:
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        attempts += 1
                        if attempts == max_attempts:
                            self.lc.logger.error(f"All {max_attempts} attempts failed: {e}")
                            raise
                        else:
                            self.lc.logger.warning(
                                f"Attempt {attempts} failed with {e}. Retrying in {delay} seconds..."
                            )
                        time.sleep(delay)
                        delay *= backoff_factor
            return wrapper_retry
        return decorator_retry

    def add_hash_column(self, input_df, hash_col_name, col_list):
        this_module = f"[{self.this_class_name}.add_hash_column()] -"
        self.validate_function_param(
            this_module,
            {
                "input_df": {
                    "input_value": input_df,
                    "data_type": "spark_dataframe",
                    "check_empty": True,
                },
                "hash_col_name": {
                    "input_value": hash_col_name,
                    "data_type": "str",
                    "check_empty": True,
                },
                "col_list": {
                    "input_value": col_list,
                    "data_type": "list",
                    "check_empty": True,
                },
            }
        )

        input_df_columns = input_df.columns
        if hash_col_name in input_df_columns:
            error_msg = (
                f"{this_module} "
                f"ValueError: {hash_col_name} "
                f"already found in "
                f"input_df.columns --> {input_df_columns}"
            )
            self.lc.logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            self.lc.logger.info(
                f"{this_module} "
                f"Creating hash column {hash_col_name} "
                f"from {col_list}, "
                f"input_df.columns --> {input_df_columns}"
            )
            ret_df = input_df.withColumn(
                hash_col_name, f.sha2(f.concat_ws("_", *col_list), 256)
            )
            self.lc.logger.info(
                f"{this_module} "
                f"Created hash column {hash_col_name} "
                f"ret_df.columns --> {ret_df.columns}"
            )
            return ret_df
        except PySparkException as e:
            error_msg = (
                f"{this_module} "
                f"hash_col_name --> {hash_col_name}, "
                f"col_list --> {col_list}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            if e.getErrorClass().split(".")[0] == "UNRESOLVED_COLUMN":
                self.lc.logger.error(
                    f"One of more columns not found "
                    f"in dataframe: {error_msg}"
                )
                raise
            else:
                self.lc.logger.error(error_msg)
                raise
        except Exception as e:
            error_msg = (
                f"{this_module} "
                f"hash_col_name --> {hash_col_name}, "
                f"col_list --> {col_list}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise

    def add_current_timestamp(self, input_df, column_name):
        this_module = f"[{self.this_class_name}.add_current_timestamp()] -"
        self.validate_function_param(
            this_module,
            {
                "input_df": {
                    "input_value": input_df,
                    "data_type": "spark_dataframe",
                    "check_empty": True,
                },
                "column_name": {
                    "input_value": column_name,
                    "data_type": "str",
                    "check_empty": True,
                },
            }
        )

        input_df_columns = input_df.columns
        if column_name in input_df_columns:
            error_msg = (
                f"{this_module} "
                f"ValueError: {column_name} "
                f"already found in "
                f"input_df.columns --> {input_df_columns}"
            )
            self.lc.logger.error(error_msg)
            raise ValueError(error_msg)
        try:
            self.lc.logger.info(
                f"{this_module} "
                f"Adding column {column_name} "
                f"input_df.columns --> {input_df_columns}"
            )
            ret_df = input_df.withColumn(column_name, f.current_timestamp())
            return ret_df
        except PySparkException as e:
            error_msg = (
                f"{this_module} "
                f"column_name --> {column_name}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = (
                f"{this_module} "
                f"column_name --> {column_name}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise

    def add_literal_column(self, input_df, column_name, column_value):
        this_module = f"[{self.this_class_name}.add_literal_column()] -"
        self.validate_function_param(
            this_module,
            {
                "input_df": {
                    "input_value": input_df,
                    "data_type": "spark_dataframe",
                    "check_empty": True,
                },
                "column_name": {
                    "input_value": column_name,
                    "data_type": "str",
                    "check_empty": True,
                },
            }
        )

        input_df_columns = input_df.columns
        if column_name in input_df_columns:
            error_msg = (
                f"{this_module} "
                f"ValueError: {column_name} "
                f"already found in "
                f"input_df.columns --> {input_df_columns}"
            )
            self.lc.logger.error(error_msg)
            raise ValueError(error_msg)
        try:
            self.lc.logger.info(
                f"{this_module} "
                f"Adding column {column_name}, "
                f"column_value --> {column_value}, "
                f"input_df.columns --> {input_df_columns}"
            )
            ret_df = input_df.withColumn(column_name, f.lit(column_value))
            return ret_df
        except PySparkException as e:
            error_msg = (
                f"{this_module} "
                f"column_name --> {column_name}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = (
                f"{this_module} "
                f"column_name --> {column_name}, "
                f"input_df.columns --> {input_df_columns}, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise

    def execute_sql_on_dataframe(self, input_df, sql_string):
        this_module = f"[{self.this_class_name}.execute_sql_on_dataframe()] -"

        # Validate parameters
        self.validate_function_param(
            this_module,
            {
                "input_df": {
                    "input_value": input_df,
                    "data_type": "spark_dataframe",
                    "check_empty": True,
                },
                "sql_string": {
                    "input_value": sql_string,
                    "data_type": "str",
                    "check_empty": True,
                },
            }
        )

        try:
            # Generate a temporary view name
            temp_view_name = f"temp_view_{str(abs(hash(input_df)))}"

            self.lc.logger.info(
                f"{this_module} Registering temp view '{temp_view_name}' with columns: {input_df.columns}"
            )

            # Register the DataFrame as a temp view
            input_df.createOrReplaceTempView(temp_view_name)

            # Replace reference to input_df with the temp view in SQL
            sql_to_execute = sql_string.format(source_data=temp_view_name)

            self.lc.logger.info(
                f"{this_module} Executing SQL: {sql_to_execute}"
            )

            # Execute the SQL query
            result_df = self.spark.sql(sql_to_execute)

            self.lc.logger.info(
                f"{this_module} Successfully executed SQL on DataFrame"
            )

            return result_df

        except AnalysisException as e:
            error_msg = (
                f"{this_module} AnalysisException while executing SQL --> {sql_string} "
                f"\nException: {e}\nTRACEBACK:\n{traceback.format_exc()}"
            )
            self.lc.logger.error(error_msg)
            raise

        except Exception as e:
            error_msg = (
                f"{this_module} Exception while executing SQL --> {sql_string} "
                f"\nException: {e}\nTRACEBACK:\n{traceback.format_exc()}"
            )
            self.lc.logger.error(error_msg)
            raise

    def select_columns(self, input_df, columns_to_select):
        this_module = f"[{self.this_class_name}.select_columns()] -"

        try:
            self.validate_function_param(
                this_module,
                {
                    "input_df": {
                        "input_value": input_df,
                        "data_type": "spark_dataframe",
                        "check_empty": True,
                    },
                    "columns_to_select": {
                        "input_value": columns_to_select,
                        "data_type": "list",
                        "check_empty": True,
                    },
                }
            )

            missing_cols = [col for col in columns_to_select if col not in input_df.columns]
            if missing_cols:
                raise ValueError(f"Missing columns in DataFrame: {missing_cols}")

            return input_df.select(*columns_to_select)

        except (AnalysisException, ValueError, TypeError) as e:
            self.lc.logger.error(f"{this_module} {e}\nTRACEBACK: {traceback.format_exc()}")
            raise

        except Exception as e:
            self.lc.logger.error(f"{this_module} Unexpected error.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

    def drop_columns(self, input_df, columns_to_drop):
        this_module = f"[{self.this_class_name}.drop_columns()] -"

        try:
            self.validate_function_param(
                this_module,
                {
                    "input_df": {
                        "input_value": input_df,
                        "data_type": "spark_dataframe",
                        "check_empty": True,
                    },
                    "columns_to_drop": {
                        "input_value": columns_to_drop,
                        "data_type": "list",
                        "check_empty": True,
                    },
                }
            )

            existing_cols = [col for col in columns_to_drop if col in input_df.columns]
            return input_df.drop(*existing_cols)

        except (AnalysisException, ValueError, TypeError) as e:
            self.lc.logger.error(f"{this_module} {e}\nTRACEBACK: {traceback.format_exc()}")
            raise

        except Exception as e:
            self.lc.logger.error(f"{this_module} Unexpected error.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

    def apply_where_clause(self, input_df, where_clause):
        this_module = f"[{self.this_class_name}.apply_where_clause()] -"

        try:
            self.validate_function_param(
                this_module,
                {
                    "input_df": {
                        "input_value": input_df,
                        "data_type": "spark_dataframe",
                        "check_empty": True,
                    },
                    "where_clause": {
                        "input_value": where_clause,
                        "data_type": "str",
                        "check_empty": True,
                    },
                }
            )

            return input_df.where(where_clause)

        except AnalysisException as e:
            self.lc.logger.error(f"{this_module} Invalid where clause: {where_clause}\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

        except Exception as e:
            self.lc.logger.error(f"{this_module} Failed to apply where clause.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

    def apply_column_transformations(self, input_df, column_mapping):
        """
        column_mapping = [
            {"source_column": "col1", "target_column": "new_col1"},
            {"source_column": "col2", "target_column": "new_col2", "target_data_type": "int"},
        ]
        """
        this_module = f"[{self.this_class_name}.apply_column_transformations()] -"

        try:
            self.validate_function_param(
                this_module,
                {
                    "input_df": {
                        "input_value": input_df,
                        "data_type": "spark_dataframe",
                        "check_empty": True,
                    },
                    "column_mapping": {
                        "input_value": column_mapping,
                        "data_type": "list",
                        "check_empty": True,
                    },
                }
            )

            for mapping in column_mapping:
                if not isinstance(mapping, dict):
                    raise ValueError(f"Each item in column_mapping must be a dict, got {type(mapping)}")

                if "source_column" not in mapping or "target_column" not in mapping:
                    raise KeyError("Each mapping must contain 'source_column' and 'target_column' keys")

                src = mapping["source_column"]
                tgt = mapping["target_column"]
                dtype = mapping.get("target_data_type")

                if src not in input_df.columns:
                    raise ValueError(f"Source column '{src}' not found in input DataFrame")

                if dtype:
                    input_df = input_df.withColumn(tgt, f.col(src).cast(dtype))
                elif src != tgt:
                    input_df = input_df.withColumnRenamed(src, tgt)

            return input_df

        except (KeyError, ValueError, TypeError) as e:
            self.lc.logger.error(f"{this_module} Invalid transformation config.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

        except AnalysisException as e:
            self.lc.logger.error(f"{this_module} Spark error during transformation.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise

        except Exception as e:
            self.lc.logger.error(f"{this_module} Unexpected error in column transformations.\n{e}\nTRACEBACK: {traceback.format_exc()}")
            raise
