from pyspark.sql import SparkSession
from datetime import datetime
import yaml
import functools
import time


from edap_common.utils.constants import *
# from edap_common.utils.log_wrapper import LogWrapper
from builtins import open


class CommonUtils:
    def __init__(self, lc):
        """
        Args:
            lc: Logger class instance
        """
        self.this_class_name = f"{type(self).__name__}"
        self.lc = lc
        # self.log_wrapper = LogWrapper(lc)

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
        except Exception as e:
            self.lc.logger.error(
                f"{this_module} Exception : "
                f"error while creating dbutils object ({e})"
            )
            raise


    def validate_function_param(self, passed_module, params_dict: dict):
        this_module = f"[{self.this_class_name}.validate_function_param()] - {passed_module} -"
        for each_param_name in params_dict.keys():
            each_param_dict = params_dict[each_param_name]
            each_param_value = each_param_dict["input_value"]
            each_param_data_type = each_param_dict["data_type"]
            each_param_check_empty = each_param_dict.get("check_empty", False)

            cur_data_type_defaults = data_type_defaults[each_param_data_type]
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
