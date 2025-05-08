import pyodbc
import struct
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from azure.identity import DefaultAzureCredential


def get_ordered_params(param_dict, param_order):
    """
    Converts a dictionary of SQL parameters into a list ordered by param_order.

    Args:
        param_dict (dict): Dictionary of parameters.
        param_order (list): Ordered list of parameter keys.

    Returns:
        list: Ordered parameter values.
    """
    return [param_dict[name] for name in param_order]


class ProcessMonitoring:
    """
    Class for monitoring ETL job and run metadata in a SQL Server database using PySpark and Azure credentials.
    """

    spark = SparkSession.getActiveSession()

    def __init__(self, lc, common_utils, job_args):
        """
        Initializes the ProcessMonitoring object.

        Args:
            lc (object): Logging context object.
            common_utils (object): Utility functions shared across the job.
            job_args (object): Job configuration and state (must support `.get()` and `.set()`).
        """
        self.this_class_name = f"{type(self).__name__}"
        self.lc = lc
        self.job_args_obj = job_args
        self.common_utils = common_utils

        self.schema = StructType([])
        self.empty_rdd = self.spark.sparkContext.emptyRDD()
        self.empty_df = self.spark.createDataFrame(self.empty_rdd, self.schema)

    def get_conn(self):
        """
        Establishes a secure connection to SQL Server using Azure AD access token.

        Returns:
            pyodbc.Connection: An authenticated database connection.
        """
        connection_string = self.job_args_obj.get("process_monitoring_conn_str")
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    def execute_query_and_get_results(self, passed_query, param_dict=None, param_order=None, fetch_results=True):
        """
        Executes a SQL query securely using parameterized input.

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

        params = get_ordered_params(param_dict, param_order) if param_dict and param_order else []

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

    def get_and_set_job_id(self, raise_exception=False):
        """
        Retrieves the job_id from the job_details table and stores it in job_args.

        Args:
            raise_exception (bool): Whether to raise an exception if job_id is not found.
        """
        table = self.job_args_obj.get("job_details_table_name")
        query = f"""
            SELECT job_id FROM {table}
            WHERE source_system = @source_system AND source = @source AND source_type = @source_type AND load_type = @load_type
        """
        param_dict = {
            "source_system": self.job_args_obj.get("source_system"),
            "source": self.job_args_obj.get("source"),
            "source_type": self.job_args_obj.get("source_type"),
            "load_type": self.job_args_obj.get("load_type")
        }
        param_order = list(param_dict.keys())
        df = self.execute_query_and_get_results(query, param_dict, param_order)
        if df.count() == 1:
            job_id = df.collect()[0]["job_id"]
            self.job_args_obj.set("job_id", job_id)
        else:
            msg = f"Expected 1 job_id but got {df.count()}"
            self.lc.logger.error(msg)
            if raise_exception:
                raise Exception(msg)

    def insert_job_details(self):
        """
        Inserts a new record into the job_details table.
        """
        table = self.job_args_obj.get("job_details_table_name")
        now = self.common_utils.get_current_time()
        query = f"""
            INSERT INTO {table}
            (_created, _modified, env, job_name, frequency, load_type, source_system, source, source_type)
            VALUES (@_created, @_modified, @env, @job_name, @frequency, @load_type, @source_system, @source, @source_type)
        """
        param_dict = {
            "_created": now,
            "_modified": now,
            "env": self.job_args_obj.get("env"),
            "job_name": self.job_args_obj.get("job_name"),
            "frequency": self.job_args_obj.get("frequency"),
            "load_type": self.job_args_obj.get("load_type"),
            "source_system": self.job_args_obj.get("source_system"),
            "source": self.job_args_obj.get("source"),
            "source_type": self.job_args_obj.get("source_type")
        }
        self.execute_query_and_get_results(query, param_dict, list(param_dict.keys()), fetch_results=False)

    def check_and_get_job_id(self):
        """
        Ensures that the job_id exists in job_args; inserts and retrieves it if missing.
        """
        self.get_and_set_job_id()
        if not self.job_args_obj.get("job_id"):
            self.insert_job_details()
            self.get_and_set_job_id(raise_exception=True)

    def check_already_processed(self, passed_job_id="", passed_run_date=""):
        """
        Checks whether the current job has already completed for the given date.

        Args:
            passed_job_id (str): Optional override for job_id.
            passed_run_date (str): Optional override for run_date.
        """
        table = self.job_args_obj.get("job_run_details_table_name")
        job_id = passed_job_id or self.job_args_obj.get("job_id")
        run_date = passed_run_date or self.job_args_obj.get("run_date")
        query = f"""
            SELECT job_id, run_status, run_error_detail FROM {table}
            WHERE job_id = @job_id AND run_date = @run_date AND UPPER(run_status) = 'COMPLETED'
        """
        param_dict = {"job_id": job_id, "run_date": run_date}
        df = self.execute_query_and_get_results(query, param_dict, list(param_dict.keys()))
        self.job_args_obj.set(f"{job_id}_completed", df.count() == 1)

    def get_and_set_run_id(self):
        """
        Retrieves the run_id for the current job run and sets it in job_args.
        """
        table = self.job_args_obj.get("job_run_details_table_name")
        query = f"""
            SELECT run_id FROM {table}
            WHERE job_id = @job_id AND run_start_time = CAST(@run_start_time AS DATETIME) AND run_date = @run_date
        """
        param_dict = {
            "job_id": self.job_args_obj.get("job_id"),
            "run_start_time": self.job_args_obj.get("run_start_time"),
            "run_date": self.job_args_obj.get("run_date")
        }
        df = self.execute_query_and_get_results(query, param_dict, list(param_dict.keys()))
        if df.count() == 1:
            run_id = df.collect()[0]["run_id"]
            self.job_args_obj.set("run_id", run_id)
        else:
            raise Exception("Unable to find a matching run_id.")

    def insert_update_job_run_status(self, passed_status, passed_comments=""):
        """
        Inserts or updates the job run status for the current job run.

        Args:
            passed_status (str): Status of the job run (e.g., 'COMPLETED', 'FAILED').
            passed_comments (str): Optional error detail or notes.
        """
        table = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_id = self.job_args_obj.get("run_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        run_row_count = self.job_args_obj.get("run_row_count") or 0
        now = self.common_utils.get_current_time()

        if not run_id:
            query = f"""
                INSERT INTO {table}
                (job_id, _created, _modified, run_start_time, run_end_time, run_date, run_row_count, run_status, run_error_detail)
                VALUES (@job_id, @_created, @_modified, @run_start_time
