import time
import pyodbc
import struct
import pandas as pd
import functools
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from azure.identity import DefaultAzureCredential


def retry_on_exception(
    exceptions=(Exception,),
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=1.0,
    logger=None
):
    """
    Retry decorator that retries a function if specified exceptions occur.

    Args:
        exceptions (tuple): Exception types to catch and retry on.
        max_attempts (int): Maximum number of attempts before giving up.
        delay_seconds (int): Initial delay between retries in seconds.
        backoff_factor (float): Multiplier to increase delay each retry.
        logger (logging.Logger): Optional logger for retry messages.

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
                        if logger:
                            logger.error(f"All {max_attempts} attempts failed: {e}")
                        raise
                    if logger:
                        logger.warning(
                            f"Attempt {attempts} failed with {e}. Retrying in {delay} seconds..."
                        )
                    time.sleep(delay)
                    delay *= backoff_factor
        return wrapper_retry
    return decorator_retry


class ProcessMonitoring:
    spark = SparkSession.getActiveSession()

    def __init__(self, lc, common_utils, job_args):
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.job_args_obj = job_args
        self.common_utils = common_utils

        self.schema = StructType([])
        self.empty_rdd = self.spark.sparkContext.emptyRDD()
        self.empty_df = self.spark.createDataFrame(self.empty_rdd, self.schema)
        self.empty_dict = {}

    @retry_on_exception(exceptions=(pyodbc.Error, ConnectionError), max_attempts=3, delay_seconds=2, backoff_factor=2.0, logger=None)
    def get_conn(self):
        """
        Establishes a secure connection to SQL Server using Azure AD access token.
        Retries up to 3 times on failure.
        """
        connection_string = self.job_args_obj.get("process_monitoring_conn_str")
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    @retry_on_exception(exceptions=(pyodbc.Error,), max_attempts=3, delay_seconds=2, backoff_factor=2.0, logger=None)
    def execute_query_and_get_results(self, passed_query, param_dict=None, param_order=None, fetch_results=True):
        """
        Executes a SQL query securely using parameterized input.
        Retries on database errors.

        Args:
            passed_query (str): SQL query string with @param placeholders.
            param_dict (dict, optional): Dictionary of parameters.
            param_order (list, optional): Order of parameters in the SQL statement.
            fetch_results (bool): If True, fetch results as a Spark DataFrame.

        Returns:
            pyspark.sql.DataFrame: Results of the query or an empty DataFrame.
        """
        dry_run = self.job_args_obj.get("dry_run")
        if dry_run:
            return self.empty_df

        params = self.get_ordered_params(param_dict, param_order) if param_dict and param_order else []

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(passed_query, params)
            if fetch_results:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                if not rows:
                    return self.empty_df
                pandas_df = pd.DataFrame.from_records(rows, columns=columns)
                return self.spark.createDataFrame(pandas_df)
            else:
                cursor.commit()
                cursor.close()
                return self.empty_df

    def get_ordered_params(self, param_dict, param_order):
        """Helper function to reorder params based on the passed order."""
        return [param_dict[key] for key in param_order]

    def get_and_set_job_id(self, raise_exception=False):
        this_module = f"[{self.this_class_name}.get_and_set_job_id()] -"
        job_details_table_name = self.job_args_obj.get("job_details_table_name")
        load_type = self.job_args_obj.get("load_type")
        source_system = self.job_args_obj.get("source_system")
        source = self.job_args_obj.get("source")
        source_type = self.job_args_obj.get("source_type")
        query_to_execute = (
            f"SELECT "
            f"job_id "
            f"FROM "
            f"{job_details_table_name} "
            f"WHERE "
            f"source_system = '{source_system}' AND "
            f"source = '{source}' AND "
            f"source_type = '{source_type}' AND "
            f"load_type = '{load_type}'"
        )
        job_details = self.execute_query_and_get_results(query_to_execute)
        job_details_count = job_details.count()
        job_details_list = job_details.collect()
        if job_details_count == 1:
            job_id = job_details_list[0]["job_id"]
            self.job_args_obj.set("jo_id", job_id)
        else:
            error_msg = (
                f"{this_module} "
                f"job_details_count --> {job_details_count}, "
                f"job_details_count != 1, "
                f"job_details_list --> {job_details_list}"
            )
            self.lc.logger.error(error_msg)
            if raise_exception:
                raise Exception(error_msg)

    def insert_job_details(self):
        this_module = f"[{self.this_class_name}.insert_job_details()] -"
        job_details_table_name = self.job_args_obj.get("job_details_table_name")
        env = self.job_args_obj.get("env")
        job_name = self.job_args_obj.get("job_name")
        frequency = self.job_args_obj.get("frequency")
        load_type = self.job_args_obj.get("load_type")
        source_system = self.job_args_obj.get("source_system")
        source = self.job_args_obj.get("source")
        source_type = self.job_args_obj.get("source_type")
        now_current_time = self.common_utils.get_current_time()
        query_to_execute = (
            f"INSERT INTO "
            f"{job_details_table_name} "
            f"(_created, _modified, "
            f"env, job_name, "
            f"frequency, load_type, "
            f"source_system, source, source_type) "
            f"VALUES("
            f"CAST('{now_current_time}' AS DATETIME), "
            f"CAST('{now_current_time}' AS DATETIME), "
            f"'{env}', '{job_name}', "
            f"'{frequency}', '{load_type}', "
            f"'{source_system}', '{source}', '{source_type}')"
        )
        self.execute_query_and_get_results(query_to_execute, fetch_results=False)

    def check_and_get_job_id(self):
        this_module = f"[{self.this_class_name}.check_and_get_job_id()] -"
        self.get_and_set_job_id()
        job_id = self.job_args_obj.get("job_id")
        if job_id == "":
            self.insert_job_details()
            self.get_and_set_job_id(raise_exception=True)

    def check_already_processed(self, passed_job_id="", passed_run_date=""):
        this_module = f"[{self.this_class_name}.insert_job_details()] -"
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_job_id": {
                    "input_value": passed_job_id,
                    "data_type": "str",
                },
                "passed_run_date": {
                    "input_value": passed_run_date,
                    "data_type": "str",
                }
            }
        )

        if passed_job_id:
            job_id = passed_job_id
        else:
            job_id = self.job_args_obj.get("job_id")

        if passed_run_date:
            run_date = passed_run_date
        else:
            run_date = self.job_args_obj.get("run_date")
        query_to_execute = (
            f"SELECT "
            f"job_id, run_status, run_error_detail "
            f"FROM "
            f"{job_run_details_table_name} "
            f"WHERE "
            f"job_id = {job_id} AND "
            f"run_date = '{run_date}' AND "
            f"UPPER(run_status) = 'COMPLETED'"
        )
        job_completed_details = self.execute_query_and_get_results(query_to_execute)
        job_already_completed = False
        if job_completed_details.count() == 1:
            job_already_completed = True
        self.job_args_obj.set(f"{job_id}_completed", job_already_completed)

    def get_and_set_run_id(self):
        this_module = f"[{self.this_class_name}.get_and_set_run_id()] -"
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        query_to_execute = (
            f"SELECT "
            f"run_id "
            f"FROM "
            f"{job_run_details_table_name} "
            f"WHERE "
            f"job_id = {job_id} AND "
            f"run_start_time = CAST('{run_start_time}' AS DATETIME) AND "
            f"run_date = '{run_date}'"
        )
        run_details = self.execute_query_and_get_results(query_to_execute)
        run_details_count = run_details.count()
        run_details_list = run_details.collect()
        if run_details_count == 1:
            run_id = self.job_args_obj.get("run_id")
            self.job_args_obj.set("run_id", run_id)
        else:
            error_msg = (
                f"{this_module} "
                f"run_details_count --> {run_details_count}, "
                f"run_details_count != 1, "
                f"run_details_list --> {run_details_list}"
            )
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

    def insert_update_job_run_status(self, passed_status, passed_comments=""):
        this_module = f"[{self.this_class_name}.insert_update_job_run_status()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_status": {
                    "input_value": passed_status,
                    "data_type": "str",
                    "check_empty": True,
                },
                "passed_comments": {
                    "input_value": passed_comments,
                    "data_type": "str",
                }
            }
        )

        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_id = self.job_args_obj.get("run_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        run_row_count = self.job_args_obj.get("run_row_count")
        if run_row_count == "":
            run_row_count = 0
        now_current_time = self.common_utils.get_current_time()
        if run_id == "":
            query_to_execute = (
                f"INSERT INTO "
                f"{job_run_details_table_name} "
                f"(job_id, _created, _modified, "
                f"run_start_time, run_end_time, "
                f"run_date, run_row_count, "
                f"run_status, run_error_detail) "
                f"VALUES({job_id}, "
                f"CAST('{now_current_time}' AS DATETIME), "
                f"CAST('{now_current_time}' AS DATETIME), "
                f"CAST('{run_start_time}' AS DATETIME), "
                f"CAST('{now_current_time}' AS DATETIME), "
                f"CAST('{run_date}' AS DATE), {run_row_count}, "
                f"'{passed_status}', '{passed_comments}')"
            )
        else:
            query_to_execute = (
                f"UPDATE "
                f"{job_run_details_table_name} "
                f"SET "
                f"_modified=CAST('{now_current_time}' AS DATETIME), "
                f"run_end_time=CAST('{now_current_time}' AS DATETIME), "
                f"run_row_count={run_row_count}, "
                f"run_status='{passed_status}', "
                f"run_error_detail='{passed_comments}' "
                f"WHERE "
                f"run_id = {run_id} AND job_id = {job_id}"
            )
        self.execute_query_and_get_results(query_to_execute, fetch_results=False)
        if run_id == "":
            self.get_and_set_run_id()
