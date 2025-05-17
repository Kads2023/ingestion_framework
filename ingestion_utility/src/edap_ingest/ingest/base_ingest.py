import traceback
from abc import abstractmethod

from pkg_resources import issue_warning
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as Spark_Dataframe

from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType, BooleanType,
    DateType, TimestampType, BinaryType,
    ArrayType, MapType, StructType, DataType, StructField,
)

from edap_ingest.utils.ingestion_defaults import *


class BaseIngest:
    """
    BaseIngest is a foundational class that manages the ingestion process for ETL pipelines
        running in a Databricks environment using PySpark. It coordinates input validation,
        configuration reading, schema formation, source/target location preparation, and
        job monitoring.

    Attributes:
            input_args_obj (object): Handler for input arguments passed to the ingestion job.
            job_args_obj (object): Handler for job arguments that evolve during execution.
            common_utils_obj (object): Utility object providing logging, dbutils handling, and helper functions.
            process_monitoring_obj (object): Object to monitor, update, and log job status (e.g., Started, Completed, Failed).
            validation_obj (object): Object responsible for validation utilities.
            dbutils (object): Databricks utility object for handling notebooks and filesystem operations.
            spark (SparkSession): Active Spark session.
    Methods:
        read_and_set_input_args():
            Reads and sets mandatory and default input parameters for the ingestion job.

        read_and_set_common_config():
            Reads a YAML file containing common configuration settings and sets them into job arguments.

        read_and_set_table_config():
            Reads a YAML file containing table-specific configuration settings and sets them into job arguments.

        exit_without_errors(passed_message):
            Gracefully exits the job execution with a success message, updating the job status.

        pre_load():
            Performs pre-load steps including input validation, configuration reading, job initialization,
            and pre-processing checks (e.g., verifying if the job has already been processed).

        form_schema_from_dict():
            Constructs a Spark `StructType` schema object dynamically based on the provided schema dictionary.

        form_source_and_target_locations():
            Forms dynamic source file paths and target table names based on the run date and job configuration.

        collate_columns_to_add():
            Collates audit and table columns to be added to the target during the ingestion process.

        load():
            Executes the load steps: prepares schema, paths, and additional columns.

        post_load():
            Updates the job status to "Completed" once data loading tasks are finalized.

        run_load():
            High-level orchestration method to run the full ingestion pipeline:
            Pre-load validations -> Load preparation -> Post-load status update.
            Catches exceptions, logs errors, and updates the job status to "Failed" if any error occurs.
    """
    def __init__(
            self,
            lc,
            input_args,
            job_args,
            common_utils,
            process_monitoring,
            validation_utils,
            dbutils=None
    ):
        """
            Initializes the BaseIngest class with the necessary objects for ingestion processing.

            Args:
                lc: log handler
                input_args (object): Object managing input parameters.
                job_args (object): Object managing runtime and job-specific parameters.
                common_utils (object): Utility object for common helper methods.
                process_monitoring (object): Object handling job monitoring and status updates.
                validation_utils (object): Object responsible for validation-related utilities.
                dbutils (object, optional): Databricks dbutils. If not provided, it is initialized internally.
        """
        self.this_class_name = "BaseIngest"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.input_args_obj = input_args
        self.job_args_obj = job_args
        self.common_utils_obj = common_utils
        self.process_monitoring_obj = process_monitoring
        self.validation_obj = validation_utils
        self.lc.logger.info(f"Inside {this_module}")
        self.raise_exception = True

        # Checks if dbutils is passed. if not
        # creates a handler for dbutils and returns the same
        self.dbutils = self.common_utils_obj.check_and_set_dbutils(dbutils)
        self.spark = SparkSession.getActiveSession()

    def read_and_set_input_args(self):
        """
            Reads mandatory and default input parameters, evaluates certain values (e.g., boolean flags),
            and sets them into the job arguments object for downstream usage.
        """
        this_module = f"[{self.this_class_name}.read_and_set_input_args()] -"
        self.lc.logger.info(f"Inside {this_module}")
        get_input_args_keys = self.input_args_obj.get_args_keys()
        for each_key in get_input_args_keys:
            each_key_value = self.input_args_obj.get(each_key)
            if each_key in input_params_to_be_converted_to_bool:
                final_key_values = self.common_utils_obj.check_and_evaluate_str_to_bool(
                    each_key_value
                )
            else:
                final_key_values = each_key_value
            self.job_args_obj.set(each_key, final_key_values)

    def read_and_set_common_config(self):
        """
            Reads a YAML configuration file specified by `common_config_file_location`
            and populates the job arguments with common configuration values.
        """
        this_module = f"[{self.this_class_name}.read_and_set_common_config()] -"
        self.lc.logger.info(f"Inside {this_module}")
        common_config_file_location = self.input_args_obj.get(
            "common_config_file_location"
        ).strip()
        common_dict = self.common_utils_obj.read_yaml_file(common_config_file_location)
        for each_key in common_dict.keys():
            each_key_value = common_dict[each_key]
            self.job_args_obj.set(each_key, each_key_value)

    def read_and_set_table_config(self):
        """
            Reads a YAML configuration file specified by `table_config_file_location`
            and populates the job arguments with table-specific settings.
        """
        this_module = f"[{self.this_class_name}.read_and_set_table_config()] -"
        self.lc.logger.info(f"Inside {this_module}")
        table_config_file_location = self.input_args_obj.get(
            "table_config_file_location"
        ).strip()
        table_dict = self.common_utils_obj.read_yaml_file(table_config_file_location)
        for each_key in table_dict.keys():
            each_key_value = table_dict[each_key]
            self.job_args_obj.set(each_key, each_key_value)

    def exit_without_errors(self, passed_message):
        """
            Updates the job status to 'Exited' with a provided message and gracefully exits the notebook.

            Args:
                passed_message (str): Message to log and pass during the notebook exit.
        """
        self.process_monitoring_obj.insert_update_job_run_status(
            "Exited", # Already_Processed
            passed_comments=passed_message
        )
        self.raise_exception = False
        self.dbutils.notebook.exit(passed_message)
        # sys.exit(0)

    def pre_load(self):
        """
            Performs pre-load checks and setups:
            - Reads and sets input parameters and configuration files.
            - Initializes job monitoring.
            - Checks if the job has already been processed for the given run date,
              and exits without processing if already completed.
        """
        this_module = f"[{self.this_class_name}.pre_load()] -"
        self.lc.logger.info(f"Inside {this_module}")
        self.input_args_obj.set_mandatory_input_params(
            mandatory_input_params
        )
        self.input_args_obj.set_default_values_for_input_params(
            default_values_for_input_params
        )
        self.read_and_set_common_config()
        self.read_and_set_table_config()
        self.read_and_set_input_args()
        self.process_monitoring_obj.check_and_get_job_id()
        job_id = self.job_args_obj.get("job_id")
        run_date = self.job_args_obj.get_mandatory("run_date")
        self.process_monitoring_obj.insert_update_job_run_status("Started")
        self.process_monitoring_obj.check_already_processed()
        check_already_processed = self.job_args_obj.get_mandatory(f"{job_id}_completed")
        if check_already_processed:
            message = (
                f"{this_module} "
                f"job_id --> {job_id} "
                f"ALREADY PROCESSED FOR "
                f"run_date --> {run_date}"
            )
            self.lc.logger.info(message)
            self.exit_without_errors(message)

    def form_schema_from_dict(self):
        """
            Dynamically constructs a Spark `StructType` schema based on the provided schema dictionary
            from the job arguments. Skips columns marked as derived.
        """
        this_module = f"[{self.this_class_name}.form_schema_from_dict()] -"
        self.lc.logger.info(f"Inside {this_module}")
        schema_dict = self.job_args_obj.get("schema")
        if schema_dict:
            struct_field_list = []
            for each_column_name in schema_dict.keys():
                each_column_dict = schema_dict[each_column_name]
                column_data_type = each_column_dict.get(
                    "data_type", default_column_data_type
                ).lower().strip()
                source_column_name = each_column_dict.get(
                    "source_column_name", each_column_name
                )
                derived_column = each_column_dict.get(
                    "derived_column", default_derived_column
                )
                if derived_column != 'True':
                    now_struct_field = StructField(
                        source_column_name,
                        self.job_args_obj.get_type(column_data_type)(),
                    )
                    struct_field_list.append(now_struct_field)
                if len(struct_field_list) != 0:
                    schema_struct = StructType(struct_field_list)
                    self.job_args_obj.set("schema_struct", schema_struct)

    def form_source_and_target_locations(self):
        """
            Constructs dynamic paths for the source data file and target table based on run date
            and configuration parameters such as folder structure and file naming conventions.
        """
        this_module = f"[{self.this_class_name}.form_source_and_target_locations()] -"
        self.lc.logger.info(f"Inside {this_module}")
        run_date = self.job_args_obj.get_mandatory("run_date")
        source_base_location = self.job_args_obj.get("source_base_location")
        source_reference_location = self.job_args_obj.get("source_reference_location")
        source_file_name_prefix = self.job_args_obj.get("source_file_name_prefix")
        source_file_extension = self.job_args_obj.get("source_file_extension")
        year, month, day = self.common_utils_obj.get_date_split(run_date)
        source_folder_date_pattern = self.job_args_obj.get("source_folder_date_pattern").format(
            year=year,
            month=month,
            day=day
        )
        source_file_name_date_pattern = self.job_args_obj.get("source_file_name_date_pattern").format(
            year=year,
            month=month,
            day=day
        )
        source_location = (
            source_base_location +
            source_reference_location +
            source_folder_date_pattern +
            source_file_name_prefix +
            source_file_name_date_pattern +
            source_file_extension
        )
        self.job_args_obj.set("source_location", source_location)
        target_catalog = self.job_args_obj.get_mandatory("target_catalog")
        target_schema = self.job_args_obj.get_mandatory("target_schema")
        target_table = self.job_args_obj.get_mandatory("target_table")
        target_location = f"{target_catalog}.{target_schema}.{target_table}"
        self.job_args_obj.set("target_location", target_location)
        quarantine_target_catalog = self.job_args_obj.get(
            "quarantine_target_catalog", target_catalog
        )
        quarantine_target_schema = self.job_args_obj.get(
            "quarantine_target_schema", target_schema
        )
        quarantine_target_table = self.job_args_obj.get(
            "quarantine_target_table", f"{target_table}_quarantine"
        )
        quarantine_target_location = (f"{quarantine_target_catalog}."
                                      f"{quarantine_target_schema}."
                                      f"{quarantine_target_table}")
        self.job_args_obj.set("quarantine_target_location", quarantine_target_location)

    def collate_columns_to_add(self):
        """
            Collates and merges audit columns and table-specific columns
            to form a unified list of columns to be added during the load.
        """
        this_module = f"[{self.this_class_name}.collate_columns_to_add()] -"
        self.lc.logger.info(f"Inside {this_module}")
        audit_columns_to_be_added = self.job_args_obj.get(
            "audit_columns_to_be_added"
        )
        table_columns_to_be_added = self.job_args_obj.get(
            "table_columns_to_be_added"
        )
        columns_to_be_added = []
        columns_to_be_added.extend(audit_columns_to_be_added)
        columns_to_be_added.extend(table_columns_to_be_added)
        self.job_args_obj.set("columns_to_be_added", columns_to_be_added)

    def check_multi_line_file_option(self):
        this_module = f"[{self.this_class_name}.check_multi_line_file_option()] -"
        multi_line_from_config = self.job_args_obj.get("multi_line").strip().lower()
        multi_line = (
            str(
                self.common_utils_obj.check_and_evaluate_str_to_bool(
                    multi_line_from_config
                )
            ).strip().lower()
        )
        self.job_args_obj.set("multi_line", multi_line)

    @abstractmethod
    def read_data_from_source(self) -> Spark_Dataframe:
        """
        Implemented as part of the specific subclass
        :return: The source data as a Spark_Dataframe
        """

    def add_derived_columns(self, data_to_add_columns) -> Spark_Dataframe:
        this_module = f"[{self.this_class_name}.add_derived_columns()] -"
        columns_to_be_added = self.job_args_obj.get("columns_to_be_added")
        self.lc.logger.info(
            f"Inside {this_module} "
            f"columns_to_be_added --> {columns_to_be_added}"
        )
        after_adding_columns = data_to_add_columns
        for each_item in columns_to_be_added:
            column_name = each_item["column_name"]
            data_type = each_item["data_type"]
            value = each_item.get("column_name", None)
            if value is None:
                function_name = each_item.get("function_name", "")
                if function_name == "hash":
                    col_list = each_item["hash_of"]
                    returned_data = self.common_utils_obj.add_hash_column(
                        after_adding_columns, column_name, col_list
                    )
                elif function_name == "current_timestamp":
                    returned_data = self.common_utils_obj.add_current_timestamp(
                        after_adding_columns, column_name
                    )
                else:
                    error_msg = (
                        f"{this_module} "
                        f"ValueError: "
                        f"UNKNOWN FUNCTION NAME "
                        f"{function_name}"
                    )
                    self.lc.logger.error(error_msg)
                    raise ValueError(error_msg)
            else:
                returned_data = self.common_utils_obj.add_literal_column(
                    after_adding_columns, column_name, value
                )
            after_adding_columns = returned_data
        return after_adding_columns

    def write_data_to_target_table(self, data_to_write):
        this_module = f"[{self.this_class_name}.write_data_to_target_table()] -"
        dry_run = self.job_args_obj.get("dry_run")
        target_location = self.job_args_obj.get_mandatory("target_location")
        run_row_count = data_to_write.count()
        self.job_args_obj.set("run_row_count", run_row_count)
        if not dry_run:
            data_to_write.write.mode("append").saveAsTable(target_location)

    def write_data_to_quarantine_table(self, validation_issues_data):
        this_module = f"[{self.this_class_name}.write_data_to_quarantine_table()] -"
        dry_run = self.job_args_obj.get("dry_run")
        quarantine_target_location = self.job_args_obj.get_mandatory(
            "quarantine_target_location"
        )
        if not dry_run:
            validation_issues_data.write.mode("append").saveAsTable(
                quarantine_target_location
            )

    def load(self):
        """
            Executes the preparation phase of loading:
            - Forms the source and target locations.
            - Constructs the data schema.
            - Prepares the list of columns to be added.
        """
        this_module = f"[{self.this_class_name}.load()] -"
        self.lc.logger.info(f"Inside {this_module}")
        self.form_source_and_target_locations()
        self.form_schema_from_dict()
        self.collate_columns_to_add()
        self.check_multi_line_file_option()
        source_data = self.read_data_from_source()
        # source_data.cache()
        source_row_counts = source_data.count()
        self.job_args_obj.set("source_row_count", source_row_counts)
        if source_row_counts > 0:
            after_adding_columns_df = self.add_derived_columns(source_data)
            self.lc.logger.debug(
                f"{this_module} "
                f"after_adding_columns_df.columns --> "
                f"{after_adding_columns_df.columns}, "
                f"after_adding_columns_df.head --> "
                f"{after_adding_columns_df.head(1)}"
            )
            # FOR FUTURE REFERENCE
            # not sure why the data is becoming empty after it is through GX
            # we can pass data_to_be_validated instead of after_adding_columns_df
            # that fixes the issue
            # data_to_be_validated = after_adding_columns_df.select("*")
            validation_succeeded, validation_output_df, validation_output_dict = (
                self.validation_obj.run_validations(
                    after_adding_columns_df,
                    self.job_args_obj.get_job_dict()
                )
            )
            validation_has_error = validation_output_dict["validation_has_error"]
            self.lc.logger.debug(
                f"{this_module} "
                f"after_adding_columns_df.columns --> "
                f"{after_adding_columns_df.columns}, "
                f"after_adding_columns_df.tail --> "
                f"{after_adding_columns_df.tail(1)}"
            )
            if not validation_has_error:
                self.write_data_to_target_table(after_adding_columns_df)
            if not validation_succeeded:
                self.process_monitoring_obj.insert_validation_run_status(
                    validation_output_dict
                )
                if not(validation_output_df.rdd.isEmpty()):
                    self.write_data_to_quarantine_table(validation_output_df)
        # source_data.unpersist()

    def post_load(self):
        """
            Marks the job run status as 'Completed' after successful ingestion processing.
        """
        this_module = f"[{self.this_class_name}.post_load()] -"
        self.lc.logger.info(f"Inside {this_module}")
        self.process_monitoring_obj.insert_update_job_run_status("Completed")

    def run_load(self):
        """
            Orchestrates the full ingestion process:
            - Executes pre-load steps (validation and setup).
            - Executes the load preparation steps.
            - Marks the job as completed upon success.

            If any step fails, logs the error, marks the job as 'Failed', and raises an exception.
        """
        this_module = f"[{self.this_class_name}.run_load()] -"
        self.lc.logger.info(f"Inside {this_module}")
        try:
            self.pre_load()
            self.load()
            self.post_load()
        except Exception as e:
            self.lc.logger.error(
                f"{this_module} failed with --> {e}"
            )
            if self.raise_exception:
                self.process_monitoring_obj.insert_update_job_run_status(
                    "Failed",
                    passed_comments=f"{e}".replace(
                        '"', ''
                    ).replace(
                        "'", ""
                    ).replace(
                        'SELECT', 'S E L E C T'
                    )
                )
                raise Exception(e)
