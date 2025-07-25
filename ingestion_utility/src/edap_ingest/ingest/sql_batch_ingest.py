from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import SparkSession, DataFrame as Spark_Dataframe
import sqlglot

class SqlBatchIngest(BaseIngest):
    """
        SqlBatchIngest executes a list of SQL files as batch steps, with monitoring,
        validation, and conditional writes to target or quarantine tables.

        Inherits from:
            BaseIngest: Core class handling Spark session, monitoring, and config I/O.

        Methods:
            run_batch(): Iterates through configured SQL steps and executes them sequentially.
        """
    def __init__(self, lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils=None):
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"Inside {this_module}")

    def read_data_from_source(self) -> Spark_Dataframe:
        # This function won’t be used directly, as each SQL file is executed step-by-step
        pass

    def run_batch(self):
        """
            Main execution method for batch SQL ingestion steps.

            For each SQL step in the config:
                - Reads SQL file.
                - Parses SQL using sqlglot.
                - Executes SQL on Spark.
                - Runs data validations.
                - Writes valid data to target table.
                - Writes invalid data to quarantine table (if validation fails).
                - Logs errors and updates monitoring status.

            Raises:
                Exception: if any step fails and raise_exception is set.
            """
        this_module = f"[{self.this_class_name}.run_batch()] -"
        self.lc.logger.info(f"Inside {this_module}")

        self.pre_load()  # Reads all configs

        sql_steps = self.job_args_obj.get("sqls")
        if not sql_steps:
            raise ValueError(f"{this_module} No SQL steps defined in the config")

        for step in sql_steps:
            step_name = step.get("name", "unnamed_step")
            try:
                self.lc.logger.info(f"{this_module} Executing step: {step_name}")

                # Step-specific setup
                self.job_args_obj.set("target_catalog", step["target_catalog"])
                self.job_args_obj.set("target_schema", step["target_schema"])
                self.job_args_obj.set("target_table", step["target_table"])
                self.job_args_obj.set("target_location", f"{step['target_catalog']}.{step['target_schema']}.{step['target_table']}")
                self.job_args_obj.set("validations", step.get("validations", []))

                sql_path = step["sql_file_location"]
                sql_text = self.common_utils_obj.read_file(sql_path)
                parsed = sqlglot.parse_one(sql_text)

                if parsed:
                    df = self.spark.sql(sql_text)
                else:
                    raise ValueError(f"SQL parsing failed for step {step_name}")

                # Run validations
                validation_succeeded, validation_df, validation_dict = self.validation_obj.run_validations(
                    df, self.job_args_obj.get_job_dict()
                )

                if not validation_dict.get("validation_has_error", False):
                    self.write_data_to_target_table(df)
                if not validation_succeeded:
                    self.process_monitoring_obj.insert_validation_run_status(validation_dict)
                    if not validation_df.rdd.isEmpty():
                        self.write_data_to_quarantine_table(validation_df)

            except Exception as e:
                self.lc.logger.error(f"{this_module} Failed at step {step_name} with error: {e}")
                self.process_monitoring_obj.insert_update_job_run_status("Failed", passed_comments=str(e))
                if self.raise_exception:
                    raise

        self.post_load()



# sql_batch:
#  - name: "step_1"
#    sql_file_location: <>
#    target_catalog: sdfsd
#    target_schema: dafsg
#    target_table: sdf
#    validations:
#      - name: schema_check
#  - name: "step_2"
#    sql_file_location: <>
#    target_catalog: sdfsd
#    target_schema: dafsg
#    target_table: tyu
#    validations:
#      - name: schema_check