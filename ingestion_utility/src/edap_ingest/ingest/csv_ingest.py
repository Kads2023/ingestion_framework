from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import DataFrame as Spark_Dataframe


class CsvIngest(BaseIngest):
    """
    CsvIngest class to manage the ingestion process for CSV files.

    Inherits from:
        BaseIngest: Base class containing common ingestion logic.

    Attributes:
        this_class_name (str): Name of the class for logging purposes.
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
        Initialize the CsvIngest class.

        Args:
            lc: Logging context object for writing logs.
            input_args: Dictionary of input arguments for the job.
            job_args: Object providing access to job configuration parameters.
            common_utils: Utility class for common transformations or helpers.
            process_monitoring: Object to monitor and report ingestion job status.
            validation_utils: Utility class for validating input data or parameters.
            dbutils (optional): Databricks utilities object, if applicable.
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"Inside {this_module}")

    def read_data_from_source(self) -> Spark_Dataframe:
        """
        Read data from the source CSV file(s) as per the job configuration.

        Uses schema from job arguments if provided, otherwise infers schema.
        Also logs relevant metadata and configuration settings.

        Returns:
            Spark_Dataframe: DataFrame created from the CSV source.
        """
        this_module = f"[{self.this_class_name}.load()] -"
        super().read_data_from_source()
        source_location = self.job_args_obj.get_mandatory("source_location")
        target_location = self.job_args_obj.get_mandatory("target_location")
        schema_struct = self.job_args_obj.get_mandatory("schema_struct")
        multi_line = self.job_args_obj.get("multi_line")

        self.lc.logger.info(
            f"Inside {this_module} "
            f"source_location --> {source_location}, "
            f"target_location --> {target_location}, "
            f"schema_struct --> {schema_struct}, "
            f"multi_line --> {multi_line}"
        )
        if schema_struct:
            data_df = (
                self.spark.read.option("header", "true")
                .option("multiline", multi_line)
                .schema(schema_struct)
                .csv(source_location)
            )
        else:
            data_df = (
                self.spark.read.option("header", "true")
                .option("multiline", multi_line)
                .option("inferSchema", "true")
                .csv(source_location)
            )
        self.lc.logger.info(
            f"{this_module} "
            f"After reading data, "
            f"data_df.columns --> {data_df.columns}"
        )
        return data_df
