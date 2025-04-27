from pyspark.sql import SparkSession
from datetime import datetime
import yaml

from edap_common.utils.constants import *
from edap_common.utils.log_wrapper import LogWrapper
from builtins import open


class CommonUtils:
    def __init__(self, lc):
        self.this_class_name = f"{type(self).__name__}"
        self.log_wrapper = LogWrapper(lc)

    def log_msg(self, passed_log_string, passed_logger_type=default_log_type):
        self.log_wrapper.log_or_print(passed_log_string, passed_logger_type)