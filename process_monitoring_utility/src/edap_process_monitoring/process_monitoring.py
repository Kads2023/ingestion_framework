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


def log_query(self, query: str, param_order: list, param_dict: dict):
    """
    Logs the executed query with parameters substituted for debugging purposes.
    This is not SQL injection safe and should only be used for debugging.
    """
    if not param_order or not param_dict:
        print(f"[DEBUG] Executing Query: {query}")
        return

    try:
        # Format parameters with simple representation
        formatted_params = [repr(param_dict[k]) for k in param_order]
        formatted_query = query
        for val in formatted_params:
            formatted_query = formatted_query.replace("?", val, 1)

        print(f"[DEBUG] Executing Query: {formatted_query}")
    except Exception as e:
        print(f"[DEBUG] Error formatting query: {e}")
        print(f"[DEBUG] Original Query: {query}, Params: {param_dict}")



class ProcessMonitoring:
    spark = SparkSession.getActiveSession()

    def __init__(self, lc, common_utils, job_args):
        self.this_class_name = f"{type(self).__name__}"
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
        Executes a SQL query securely using parameterized input with '?' placeholders.

        Args:
            passed_query (str): SQL query string with '?' placeholders.
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
        """
        Retrieves and sets the job ID from the job details table based on provided parameters.
        """
        job_details_table_name = self.job_args_obj.get("job_details_table_name")
        load_type = self.job_args_obj.get("load_type")
        source_system = self.job_args_obj.get("source_system")
        source = self.job_args_obj.get("source")
        source_type = self.job_args_obj.get("source_type")
        query_to_execute = (
            f"SELECT job_id FROM {job_details_table_name} WHERE source_system = ? AND source = ? AND source_type = ? AND load_type = ?"
        )
        param_order = ["source_system", "source", "source_type", "load_type"]
        param_dict = {
            "source_system": source_system,
            "source": source,
            "source_type": source_type,
            "load_type": load_type,
        }
        job_details = self.execute_query_and_get_results(query_to_execute, param_dict=param_dict, param_order=param_order)
        job_details_count = job_details.count()
        job_details_list = job_details.collect()
        if job_details_count == 1:
            job_id = job_details_list[0]["job_id"]
            self.job_args_obj.set("job_id", job_id)
        else:
            error_msg = f"job_details_count --> {job_details_count}, job_details_count != 1, job_details_list --> {job_details_list}"
            self.lc.logger.error(error_msg)
            if raise_exception:
                raise Exception(error_msg)

    def insert_job_details(self):
        """
        Inserts job details into the job details table.
        """
        job_details_table_name = self.job_args_obj.get("job_details_table_name")
        now = self.common_utils.get_current_time()
        fields = ["env", "job_name", "frequency", "load_type", "source_system", "source", "source_type"]
        values = [self.job_args_obj.get(field) for field in fields]
        param_dict = dict(zip(fields, values))
        param_dict.update({"_created": now, "_modified": now})
        query_to_execute = (
            f"INSERT INTO {job_details_table_name} (_created, _modified, env, job_name, frequency, load_type, source_system, source, source_type) "
            f"VALUES(CAST(? AS DATETIME), CAST(? AS DATETIME), ?, ?, ?, ?, ?, ?, ?)"
        )
        param_order = ["_created", "_modified"] + fields
        self.execute_query_and_get_results(query_to_execute, param_dict=param_dict, param_order=param_order, fetch_results=False)

    def check_already_processed(self, passed_job_id="", passed_run_date=""):
        """
        Checks if a job has already been processed based on job ID and run date.
        """
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = passed_job_id or self.job_args_obj.get("job_id")
        run_date = passed_run_date or self.job_args_obj.get("run_date")
        query_to_execute = (
            f"SELECT job_id, run_status, run_error_detail FROM {job_run_details_table_name} WHERE job_id = ? AND run_date = ? AND UPPER(run_status) = 'COMPLETED'"
        )
        param_dict = {"job_id": job_id, "run_date": run_date}
        param_order = ["job_id", "run_date"]
        job_completed_details = self.execute_query_and_get_results(query_to_execute, param_dict=param_dict, param_order=param_order)
        job_already_completed = job_completed_details.count() == 1
        self.job_args_obj.set(f"{job_id}_completed", job_already_completed)

    def get_and_set_run_id(self):
        """
        Retrieves and sets the run ID for the current job run.
        """
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        query_to_execute = (
            f"SELECT run_id FROM {job_run_details_table_name} WHERE job_id = ? AND run_start_time = CAST(? AS DATETIME) AND run_date = ?"
        )
        param_dict = {"job_id": job_id, "run_start_time": run_start_time, "run_date": run_date}
        param_order = ["job_id", "run_start_time", "run_date"]
        run_details = self.execute_query_and_get_results(query_to_execute, param_dict=param_dict, param_order=param_order)
        if run_details.count() == 1:
            self.job_args_obj.set("run_id", run_details.collect()[0]["run_id"])
        else:
            error_msg = f"Run ID not found or not unique. run_details_count --> {run_details.count()}"
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

    def insert_update_job_run_status(self, passed_status, passed_comments=""):
        """
        Inserts or updates the job run status in the job run details table.
        """
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_id = self.job_args_obj.get("run_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        run_row_count = self.job_args_obj.get("run_row_count") or 0
        now = self.common_utils.get_current_time()

        if run_id == "":
            query_to_execute = (
                f"INSERT INTO {job_run_details_table_name} "
                f"(job_id, _created, _modified, run_start_time, run_end_time, run_date, run_row_count, run_status, run_error_detail) "
                f"VALUES(?, CAST(? AS DATETIME), CAST(? AS DATETIME), CAST(? AS DATETIME), CAST(? AS DATETIME), CAST(? AS DATE), ?, ?, ?)"
            )
            param_order = ["job_id", "_created", "_modified", "run_start_time", "run_end_time", "run_date", "run_row_count", "run_status", "run_error_detail"]
            param_dict = {
                "job_id": job_id,
                "_created": now,
                "_modified": now,
                "run_start_time": run_start_time,
                "run_end_time": now,
                "run_date": run_date,
                "run_row_count": run_row_count,
                "run_status": passed_status,
                "run_error_detail": passed_comments,
            }
        else:
            query_to_execute = (
                f"UPDATE {job_run_details_table_name} SET _modified=CAST(? AS DATETIME), run_end_time=CAST(? AS DATETIME), "
                f"run_row_count=?, run_status=?, run_error_detail=? WHERE run_id = ? AND job_id = ?"
            )
            param_order = ["_modified", "run_end_time", "run_row_count", "run_status", "run_error_detail", "run_id", "job_id"]
            param_dict = {
                "_modified": now,
                "run_end_time": now,
                "run_row_count": run_row_count,
                "run_status": passed_status,
                "run_error_detail": passed_comments,
                "run_id": run_id,
                "job_id": job_id,
            }

        self.execute_query_and_get_results(query_to_execute, param_dict=param_dict, param_order=param_order, fetch_results=False)
        if run_id == "":
            self.get_and_set_run_id()
