import struct
import json
import pandas as pd
import sqlalchemy as sa
import traceback

from urllib import parse
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from azure.identity import ManagedIdentityCredential

from .enums import RunStatus, ValidationRunStatus
from .process_monitoring_defaults import *


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
        self._engine = None   # <-- class-level cache for engine

    def retry_any_function(self, func, *args, **kwargs):
        decorated_func = self.common_utils.retry_on_exception(
            max_attempts=MAX_ATTEMPTS,
            delay_seconds=DELAY_SECONDS,
            backoff_factor=BACKOFF_FACTOR,
        )(func)
        result = decorated_func(*args, **kwargs)
        return result

    def _create_engine(self):
        """Helper to create a new SQLAlchemy engine with fresh token."""
        connection_string = self.job_args_obj.get("process_monitoring_conn_str")
        params = parse.quote(connection_string)

        credential = ManagedIdentityCredential()
        token_bytes = credential.get_token(
            "https://database.windows.net/.default"
        ).token.encode("UTF-16-LE")

        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256

        return create_engine(
            "mssql+pyodbc:///?odbc_connect={0}".format(params),
            connect_args={"attrs_before": {SQL_COPT_SS_ACCESS_TOKEN: token_struct}},
            pool_pre_ping=True,   # <-- checks connections before using them
        )

    def get_conn(self):
        """Get reusable engine, refresh if needed."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def execute_query_and_get_results(
        self,
        passed_query,
        passed_query_params,
        fetch_results=True,
        query_params_type="dict",
        calling_module=""
    ):
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
            if query_to_check.startswith(("SELECT", "INSERT")) or (
                query_to_check.startswith("UPDATE") and "WHERE" in query_to_check
            ):
                try:
                    with self.get_conn().begin() as conn:
                        result = conn.execute(sa.text(passed_query), passed_query_params)
                        if fetch_results:
                            rows = result.fetchall()
                            columns = [desc for desc in result.keys()]
                            if rows:
                                pandas_df = pd.DataFrame.from_records(rows, columns=columns)
                                read_df = self.spark.createDataFrame(pandas_df)
                        conn.commit()
                except OperationalError:
                    # Refresh engine on failure (token expired etc.)
                    self.lc.logger.warning("Connection expired, refreshing token and engine...")
                    raise
            else:
                error_msg = f"CANNOT EXECUTE THE QUERY"
                self.lc.logger.info(error_msg)
                raise ValueError(error_msg)

        return read_df
