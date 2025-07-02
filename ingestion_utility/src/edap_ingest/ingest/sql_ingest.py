from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import DataFrame as Spark_Dataframe

import sqlglot
from pyspark.sql.utils import AnalysisException


class SqlIngest(BaseIngest):
    def __init__(self, lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils=None):
        self.this_class_name = f"{type(self).__name__}"
        super().__init__(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"[{self.this_class_name}.__init__()] - Initialized")

    def read_sql_from_file(self) -> str:
        sql_file_path = self.job_args_obj.get_mandatory("sql_file_location")
        self.lc.logger.info(f"[{self.this_class_name}.read_sql_from_file()] - Reading SQL from {sql_file_path}")

        if not sql_file_path.lower().endswith(".sql"):
            raise ValueError(f"Invalid file extension for SQL file: {sql_file_path}. Expected a '.sql' file.")

        return self.common_utils_obj.read_file_as_string(sql_file_path)

    def validate_sql(self, sql: str):
        forbidden_keywords = ["delete", "drop", "truncate"]
        lowered_sql = sql.lower()

        for keyword in forbidden_keywords:
            if keyword in lowered_sql:
                raise ValueError(f"SQL contains forbidden keyword: {keyword.upper()}")

        if not any(stmt in lowered_sql for stmt in ["select", "merge", "insert", "update"]):
            raise ValueError("SQL must contain at least one supported operation (SELECT, MERGE, INSERT, UPDATE)")


    def validate_sql_with_parser(self, sql: str):
        """
        Validates that the SQL contains only allowed statements.

        Allowed: SELECT, INSERT, UPDATE, MERGE, CREATE
        Disallowed: DELETE, DROP, TRUNCATE, etc.

        Args:
            sql (str): SQL string to validate

        Raises:
            ValueError: If disallowed or unsupported SQL statements are found
        """
        try:
            # sqlglot can parse multiple statements separated by semicolon
            parsed_statements = sqlglot.parse(sql)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")

        allowed_statements = {"SELECT", "INSERT", "UPDATE", "MERGE", "CREATE"}

        for stmt in parsed_statements:
            statement_type = stmt.key.upper()
            if statement_type not in allowed_statements:
                raise ValueError(
                    f"Disallowed SQL operation detected: {statement_type}. "
                    f"Only {', '.join(allowed_statements)} are allowed."
                )

    def read_data_from_source(self) -> Spark_Dataframe:
        this_module = f"[{self.this_class_name}.read_data_from_source()] -"
        self.lc.logger.info(f"Inside {this_module}")

        sql_text = self.read_sql_from_file()
        self.validate_sql_with_parser(sql_text)

        try:
            result_df = self.spark.sql(sql_text)
            return result_df
        except AnalysisException as ae:
            self.lc.logger.error(f"{this_module} Spark SQL analysis error: {ae}")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} Unexpected error executing SQL: {e}")
            raise