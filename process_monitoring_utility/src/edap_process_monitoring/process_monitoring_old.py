import struct
import json
import pandas as pd

from urllib import parse
from sqlalchemy import create_engine
import sqlalchemy as sa

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from azure.identity import ManagedIdentityCredential

from .enums import RunStatus, ValidationRunStatus


MAX_ATTEMPTS = 3
DELAY_SECONDS = 2
BACKOFF_FACTOR = 1.0


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

    def retry_any_function(self, func, *args, **kwargs):
        decorated_func = self.common_utils.retry_on_exception(
            max_attempts=MAX_ATTEMPTS,
            delay_seconds=DELAY_SECONDS,
            backoff_factor=BACKOFF_FACTOR,
        )(func)
        result = decorated_func(*args, **kwargs)
        return result

    # def retry_helper(self, **kwargs):
    #     return self.common_utils.retry_on_exception(**kwargs)

    # @retry_helper(exceptions=(
    #     OperationalError,
    #     DBAPIError,
    #     pyodbc.InterfaceError,
    #     pyodbc.OperationalError,
    #     ConnectionError,
    #     ClientAuthenticationError,
    #     CredentialUnavailableError,
    #     TimeoutError,
    # ), max_attempts=3, delay_seconds=2)
    def get_conn(self):
        """
        Establishes a secure connection to SQL Server using Azure AD access token.
        Retries up to 3 times on failure.
        """
        connection_string = self.job_args_obj.get("process_monitoring_conn_str")
        params = parse.quote(connection_string)
        credential = ManagedIdentityCredential()
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        # This connection option is defined by microsoft in msodbcsql.h
        return create_engine(
            "mssql+pyodbc:///?odbc_connect={0}".format(params),
            connect_args={"attrs_before": {SQL_COPT_SS_ACCESS_TOKEN: token_struct}},
        )

    # @retry_helper(exceptions=(
    #     OperationalError,
    #     DBAPIError,  # filtered if needed
    #     pyodbc.InterfaceError,
    #     pyodbc.OperationalError,
    #     ConnectionError,
    #     ClientAuthenticationError,
    #     CredentialUnavailableError,
    #     TimeoutError,
    #     SA_TimeoutError,
    # ), max_attempts=3, delay_seconds=2)
    def execute_query_and_get_results(
            self,
            passed_query,
            passed_query_params,
            fetch_results=True,
            query_params_type="dict",
            calling_module=""
    ):
        """
        Executes a SQL query securely using parameterized input with '?' placeholders.

        Args:
            passed_query (str): SQL query string with '?' placeholders.
            passed_query_params (dict, optional): Dictionary of parameters.
            fetch_results (bool): If True, fetch results as a Spark DataFrame.
            query_params_type (str): If dict, runs a single query,
                If list of dict it runs executemany internally
            calling_module (str): The module from which it is called

        Returns:
            pyspark.sql.DataFrame: Results of the query or an empty DataFrame.
        """
        final_calling_module = ""
        if calling_module:
            final_calling_module = f" CALLED FROM {calling_module}"
        this_module = f"[{self.this_class_name}.execute_query_and_get_results()] -{final_calling_module}"
        dry_run = self.job_args_obj.get("dry_run")
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_query": {"input_value": passed_query, "data_type": "str", "check_empty": True},
                "passed_query_params": {"input_value": passed_query_params, "data_type": query_params_type},
            }
        )
        read_df = self.empty_df
        if not dry_run:
            query_to_check = passed_query.strip().upper()
            if (
                query_to_check.startswith("SELECT") or
                query_to_check.startswith("INSERT") or
                    (query_to_check.startswith("UPDATE") and ("WHERE" in query_to_check))
            ):
                with self.get_conn().begin() as conn:
                    result = conn.execute(sa.text(passed_query), passed_query_params)
                    if fetch_results:
                        rows = result.fetchall()
                        columns = [desc for desc in result.keys()]
                        if rows:
                            pandas_df = pd.DataFrame.from_records(rows, columns=columns)
                            read_df = self.spark.createDataFrame(pandas_df)
                    conn.commit()
                    conn.close()
            else:
                error_msg = f"CANNOT EXECUTE THE QUERY"
                self.lc.logger.info(error_msg)
                raise ValueError(error_msg)
        return read_df

    def get_and_set_job_id(self, raise_exception=False):
        """
        Retrieves and sets the job ID from the job details table based on provided parameters.
        """
        this_module = f"[{self.this_class_name}.get_and_set_job_id()] -"
        dry_run = self.job_args_obj.get("dry_run")
        job_details_table_name = self.job_args_obj.get("job_details_table_name")
        load_type = self.job_args_obj.get("load_type")
        source_system = self.job_args_obj.get("source_system")
        source = self.job_args_obj.get("source")
        source_type = self.job_args_obj.get("source_type")
        query_to_execute = (
            f"SELECT job_id FROM {job_details_table_name} WHERE source_system = :source_system AND source = :source AND source_type = :source_type AND load_type = :load_type"
        )
        param_dict = {
            "source_system": source_system,
            "source": source,
            "source_type": source_type,
            "load_type": load_type,
        }
        job_details = self.retry_any_function(
            self.execute_query_and_get_results,
            query_to_execute,
            passed_query_params=param_dict,
            calling_module=this_module
        )
        job_details_count = job_details.count()
        job_details_list = job_details.collect()
        if job_details_count == 1:
            job_id = job_details_list[0]["job_id"]
            self.job_args_obj.set("job_id", job_id)
        else:
            error_msg = f"{this_module} job_details_count --> {job_details_count}, job_details_count != 1, job_details_list --> {job_details_list}"
            self.lc.logger.error(error_msg)
            if raise_exception:
                if dry_run:
                    self.job_args_obj.set("job_id", "dummmy_job_id")
                else:
                    raise Exception(error_msg)

    def insert_job_details(self):
        """
        Inserts job details into the job details table.
        """
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
            f"INSERT INTO {job_details_table_name} (_created, _modified, env, job_name, frequency, load_type, source_system, source, source_type) "
            f"VALUES(CAST(:_created AS DATETIME), CAST(:_modified AS DATETIME), :env, :job_name, :frequency, :load_type, :source_system, :source, :source_type)"
        )
        param_dict = {
            "_created": now_current_time,
            "_modified": now_current_time,
            "env": env,
            "job_name": job_name,
            "frequency": frequency,
            "load_type": load_type,
            "source_system": source_system,
            "source": source,
            "source_type": source_type,
        }
        self.retry_any_function(
            self.execute_query_and_get_results,
            query_to_execute,
            passed_query_params=param_dict,
            fetch_results=False
        )

    def check_and_get_job_id(self):
        """
        Checks and retrieves the job ID if not already set.
        """
        this_module = f"[{self.this_class_name}.check_and_get_job_id()] -"
        self.get_and_set_job_id()
        job_id = self.job_args_obj.get("job_id")
        if job_id == "":
            self.insert_job_details()
            self.get_and_set_job_id(raise_exception=True)

    def check_already_processed(self, passed_job_id="", passed_run_date=""):
        """
        Checks if a job has already been processed based on job ID and run date.
        """
        this_module = f"[{self.this_class_name}.check_already_processed()] -"
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_job_id": {"input_value": passed_job_id, "data_type": "str"},
                "passed_run_date": {"input_value": passed_run_date, "data_type": "str"},
            },
        )
        if passed_job_id:
            job_id = passed_job_id
        else:
            job_id = self.job_args_obj.get("job_id")

        if passed_run_date:
            run_date = passed_run_date
        else:
            run_date = self.job_args_obj.get("run_date")
        run_id = self.job_args_obj.get("run_id")
        query_to_execute = (
            f"SELECT "
            f"top_record.* "
            f"FROM "
            f"("
            f"SELECT TOP 1 "
            f"run_id, job_id, run_date, "
            f"run_start_time, run_status, run_error_detail "
            f"FROM "
            f"{job_run_details_table_name} "
            f"WHERE "
            f"UPPER(job_id) = UPPER(:job_id) AND "
            f"UPPER(run_id) != UPPER(:run_id) AND "
            f"run_date = :run_date "
            f"ORDER BY _modified DESC "
            f") top_record "
            f"WHERE "
            f"UPPER(top_record.run_status) in "
            f"("
            f"'{RunStatus.COMPLETED.value}', "
            f"'{RunStatus.COMPLETED_WITH_VAL_WARN.value}', "
            f"'{RunStatus.ALREADY_COMPLETED.value}'"
            f")"
        )
        param_dict = {"job_id": job_id, "run_id": run_id, "run_date": run_date}
        job_completed_details = self.retry_any_function(
            self.execute_query_and_get_results,
            query_to_execute,
            passed_query_params=param_dict,
            calling_module=this_module
        )
        job_already_completed = False
        if job_completed_details.count() == 1:
            job_already_completed = True
        self.job_args_obj.set(f"{job_id}_completed", job_already_completed)

    def check_duplicate_start(
            self,
            passed_job_id="",
            passed_run_date="",
            passed_wait_before_reprocessing_in_seconds=""
    ):
        """
        Checks if a job is parallely started for the job ID and run date.
        """
        this_module = f"[{self.this_class_name}.check_duplicate_start()] -"
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_job_id": {"input_value": passed_job_id, "data_type": "str"},
                "passed_run_date": {"input_value": passed_run_date, "data_type": "str"},
            },
        )
        if passed_job_id:
            job_id = passed_job_id
        else:
            job_id = self.job_args_obj.get("job_id")

        if passed_run_date:
            run_date = passed_run_date
        else:
            run_date = self.job_args_obj.get("run_date")
        if passed_wait_before_reprocessing_in_seconds:
            wait_before_reprocessing_in_seconds = int(
                passed_wait_before_reprocessing_in_seconds
            )
        else:
            wait_before_reprocessing_in_seconds = int(
                self.job_args_obj.get("wait_before_reprocessing_in_seconds")
            )
        run_id = self.job_args_obj.get("run_id")
        run_start_time = self.job_args_obj.get("run_start_time")
        query_to_execute = (
            f"SELECT "
            f"top_record.* "
            f"FROM "
            f"("
            f"SELECT TOP 1 "
            f"run_id, job_id, run_date, "
            f"run_start_time, run_status, run_error_detail,"
            f"_created, _modified,"
            f"CAST(:run_start_time AS DATETIME) AS current_run_start_time, "
            f":run_id as current_run_id, "
            f"DATEDIFF("
            f"second, "
            f"_modified, "
            f"CAST(:run_start_time AS DATETIME)"
            f") AS difference_in_seconds "
            f"FROM "
            f"{job_run_details_table_name} "
            f"WHERE "
            f"UPPER(job_id) = UPPER(:job_id) AND "
            f"UPPER(run_id) != UPPER(:run_id) AND "
            f"run_date = :run_date "
            f"ORDER BY _modified DESC "
            f") top_record "
            f"WHERE "
            f"top_record.difference_in_seconds <= "
            f":wait_before_reprocessing_in_seconds AND "
            f"UPPER(top_record.run_status) in "
            f"("
            f"'{RunStatus.STARTED.value}', "
            f"'{RunStatus.READING_SOURCE_DATA.value}'"
            f")"
        )
        param_dict = {
            "job_id": job_id,
            "run_id": run_id,
            "run_date": run_date,
            "run_start_time": run_start_time,
            "wait_before_reprocessing_in_seconds": wait_before_reprocessing_in_seconds
        }
        job_duplicate_start_details = self.retry_any_function(
            self.execute_query_and_get_results,
            query_to_execute,
            passed_query_params=param_dict,
            calling_module=this_module
        )

        job_duplicate_start = False
        if job_duplicate_start_details.count() == 1:
            job_duplicate_start = True
        self.job_args_obj.set(f"{job_id}_duplicate_start", job_duplicate_start)

    def get_and_set_run_id(self):
        """
        Retrieves and sets the run ID for the current job run.
        """
        this_module = f"[{self.this_class_name}.get_and_set_run_id()] -"
        dry_run = self.job_args_obj.get("dry_run")
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")

        query_to_execute = (
            f"SELECT run_id FROM {job_run_details_table_name} WHERE job_id = :job_id AND run_start_time = CAST(:run_start_time AS DATETIME) AND run_date = :run_date"
        )
        param_dict = {"job_id": job_id, "run_start_time": run_start_time, "run_date": run_date}
        run_details = self.execute_query_and_get_results(query_to_execute, passed_query_params=param_dict)
        run_details_count = run_details.count()
        run_details_list = run_details.collect()

        if run_details_count == 1:
            run_id = self.job_args_obj.get("run_id")
            self.job_args_obj.set("run_id", run_id)
        else:
            error_msg = (
                f"{this_module} run_details_count --> {run_details_count}, run_details_count != 1, run_details_list --> {run_details_list}"
            )
            self.lc.logger.error(error_msg)
            if dry_run:
                self.job_args_obj.set("run_id", "dummy_run_id")
            else:
                raise Exception(error_msg)

    def insert_validation_run_status(self, validation_run_dict):
        this_module = f"[{self.this_class_name}.insert_validation_run_status()] -"
        if validation_run_dict == {}:
            self.lc.logger.info(
                f"{this_module} "
                f"No Validation run failures to write, SKIPPING"
            )
            return

        validation_run_details_table_name = self.job_args_obj.get("validation_run_details_table_name")
        run_id = self.job_args_obj.get("run_id")
        validation_start_time = self.job_args_obj.get("validation_start_time")
        validation_end_time = self.job_args_obj.get("validation_end_time")
        now_current_time = self.common_utils.get_current_time()

        query_to_execute = (
            f"INSERT INTO "
            f"{validation_run_details_table_name} "
            f"(run_id, _created, _modified, validation_type, "
            f"validation_gx_check_class, validation_rule, "
            f"validation_column_name, validation_start_time, validation_end_time, "
            f"validation_failed_row_count, validation_status, validation_error_detail) "
            f"VALUES("
            f":run_id, "
            f"CAST(:_created AS DATETIME), "
            f"CAST(:_modified AS DATETIME), "
            f":validation_type, "
            f""
            f")"
        )

    def insert_update_job_run_status(self, passed_status, passed_comments=""):
        """
        Inserts or updates the job run status in the job run details table.
        """
        this_module = f"[{self.this_class_name}.insert_update_job_run_status()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_status": {"input_value": passed_status, "data_type": "str", "check_empty": True},
                "passed_comments": {"input_value": passed_comments, "data_type": "str"},
            },
        )
        job_run_details_table_name = self.job_args_obj.get("job_run_details_table_name")
        job_id = self.job_args_obj.get("job_id")
        run_id = self.job_args_obj.get("run_id")
        run_date = self.job_args_obj.get("run_date")
        run_start_time = self.job_args_obj.get("run_start_time")
        run_row_count = self.job_args_obj.get("run_row_count") or 0
        now_current_time = self.common_utils.get_current_time()

        if run_id == "":
            query_to_execute = (
                f"INSERT INTO {job_run_details_table_name} "
                f"(job_id, _created, _modified, run_start_time, run_end_time, run_date, run_row_count, run_status, run_error_detail) "
                f"VALUES(:job_id, CAST(:_created AS DATETIME), CAST(:_modified AS DATETIME), CAST(:run_start_time AS DATETIME), CAST(:run_end_time AS DATETIME), CAST(:run_date AS DATE), :run_row_count, :run_status, :run_error_detail)"
            )
            param_dict = {
                "job_id": job_id,
                "_created": now_current_time,
                "_modified": now_current_time,
                "run_start_time": run_start_time,
                "run_end_time": now_current_time,
                "run_date": run_date,
                "run_row_count": run_row_count,
                "run_status": passed_status,
                "run_error_detail": passed_comments,
            }
        else:
            query_to_execute = (
                f"UPDATE {job_run_details_table_name} SET _modified=CAST(:_modified AS DATETIME), run_end_time=CAST(:run_end_time AS DATETIME), "
                f"run_row_count=:run_row_count, run_status=:run_status, run_error_detail=:run_error_detail WHERE run_id = :run_id AND job_id = :job_id"
            )
            param_dict = {
                "_modified": now_current_time,
                "run_end_time": now_current_time,
                "run_row_count": run_row_count,
                "run_status": passed_status,
                "run_error_detail": passed_comments,
                "run_id": run_id,
                "job_id": job_id,
            }

        self.execute_query_and_get_results(query_to_execute, passed_query_params=param_dict, fetch_results=False)
        if run_id == "":
            self.get_and_set_run_id()
