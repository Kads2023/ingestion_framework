"""
CsvIngest module for reading, transforming, and loading CSV files into a target data store.

This class extends the BaseIngest functionality, adding specific logic to handle CSV sources.
It handles reading CSV files with or without a provided schema, validates the data,
and writes it to the specified target, with optional dry run capability.

Classes:
    CsvIngest: Inherits from BaseIngest and implements CSV-specific ingestion logic.
"""

from edap_ingest.ingest.base_ingest import BaseIngest

from pyspark.sql import DataFrame as Spark_Dataframe


class CsvIngest(BaseIngest):
    """
    CsvIngest class to manage the ingestion process for CSV files.

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
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"Inside {this_module}")

    def read_data_from_source(self) -> Spark_Dataframe:
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
