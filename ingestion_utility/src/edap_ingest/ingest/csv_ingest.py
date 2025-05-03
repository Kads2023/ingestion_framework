"""
CsvIngest module for reading, transforming, and loading CSV files into a target data store.

This class extends the BaseIngest functionality, adding specific logic to handle CSV sources.
It handles reading CSV files with or without a provided schema, validates the data,
and writes it to the specified target, with optional dry run capability.

Classes:
    CsvIngest: Inherits from BaseIngest and implements CSV-specific ingestion logic.
"""

from edap_ingest.ingest.base_ingest import BaseIngest


class CsvIngest(BaseIngest):
    """
    CsvIngest class to manage the ingestion process for CSV files.

    Attributes:
        this_class_name (str): Name of the class for logging purposes.
    """

    def __init__(
        self,
        input_args,
        job_args,
        common_utils,
        process_monitoring,
        validation_utils,
        dbutils=None
    ):
        """
        Initialize a CsvIngest instance.

        Args:
            input_args (object): Object containing input arguments.
            job_args (object): Object containing job-specific arguments.
            common_utils (object): Common utilities object for helper methods.
            process_monitoring (object): Process monitoring utilities.
            validation_utils (object): Validation utilities for data checks.
            dbutils (object, optional): Databricks utilities object. Defaults to None.
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"Inside {this_module}")

    def read_and_set_input_args(self):
        """
        Read and set mandatory and default input arguments for the ingestion job.
        """
        this_module = f"[{self.this_class_name}.read_and_set_input_args()] -"
        super().read_and_set_input_args()
        self.lc.logger.info(f"Inside {this_module}")

    def read_and_set_common_config(self):
        """
        Read and set the common configuration values required for ingestion.
        """
        this_module = f"[{self.this_class_name}.read_and_set_common_config()] -"
        super().read_and_set_common_config()
        self.lc.logger.info(f"Inside {this_module}")

    def read_and_set_table_config(self):
        """
        Read and set the table-specific configuration for the ingestion job.
        """
        this_module = f"[{self.this_class_name}.read_and_set_table_config()] -"
        super().read_and_set_table_config()
        self.lc.logger.info(f"Inside {this_module}")

    def pre_load(self):
        """
        Perform preliminary checks and setups before loading data.
        """
        this_module = f"[{self.this_class_name}.pre_load()] -"
        super().pre_load()
        self.lc.logger.info(f"Inside {this_module}")

    def form_schema_from_dict(self):
        """
        Form a Spark schema structure from a dictionary configuration if provided.
        """
        this_module = f"[{self.this_class_name}.form_schema_from_dict()] -"
        super().form_schema_from_dict()
        self.lc.logger.info(f"Inside {this_module}")

    def form_source_and_target_locations(self):
        """
        Construct the source file path and target table names based on provided configuration and patterns.
        """
        this_module = f"[{self.this_class_name}.form_source_and_target_locations()] -"
        super().form_source_and_target_locations()
        self.lc.logger.info(f"Inside {this_module}")

    def collate_columns_to_add(self):
        """
        Collate additional columns to be added to the dataset from configuration.
        """
        this_module = f"[{self.this_class_name}.collate_columns_to_add()] -"
        super().collate_columns_to_add()
        self.lc.logger.info(f"Inside {this_module}")

    def load(self):
        """
        Load the CSV file, validate the data, and write it to the target table.

        - Reads the CSV from source location, applying schema if provided.
        - Runs validations on the loaded dataset.
        - Saves the dataset into the target table if not a dry run.

        Raises:
            Exception: If reading, validation, or writing fails.
        """
        this_module = f"[{self.this_class_name}.load()] -"
        super().load()

        dry_run = self.job_args_obj.get("dry_run")
        source_location = self.job_args_obj.get("source_location")
        target_location = self.job_args_obj.get("target_location")
        schema_struct = self.job_args_obj.get("schema_struct")

        self.lc.logger.info(
            f"Inside {this_module} "
            f"dry_run --> {dry_run}, "
            f"source_location --> {source_location}, "
            f"target_location --> {target_location}, "
            f"schema_struct --> {schema_struct}"
        )

        if schema_struct:
            source_data = self.spark.read.load(
                source_location,
                format="csv",
                header="true"
            ).schema(schema_struct)
        else:
            source_data = self.spark.read.load(
                source_location,
                format="csv",
                inferSchema="true",
                header="true"
            )

        source_row_counts = source_data.count()
        self.job_args_obj.set("source_row_count", source_row_counts)
        self.job_args_obj.set("run_row_count", source_row_counts)

        columns_to_be_added = self.job_args_obj.get("columns_to_be_added")
        self.lc.logger.info(
            f"Inside {this_module} "
            f"columns_to_be_added --> {columns_to_be_added}"
        )

        self.validation_obj.run_validations(source_data)

        if not dry_run:
            source_data.write.mode("append").saveAsTable(target_location)

    def post_load(self):
        """
        Perform any necessary finalization steps after data loading completes.
        """
        this_module = f"[{self.this_class_name}.post_load()] -"
        super().post_load()
        self.lc.logger.info(f"Inside {this_module}")

    def run_load(self):
        """
        Execute the full ingestion workflow: pre-load, load, and post-load operations.
        """
        this_module = f"[{self.this_class_name}.run_load()] -"
        super().run_load()
        self.lc.logger.info(f"Inside {this_module}")
