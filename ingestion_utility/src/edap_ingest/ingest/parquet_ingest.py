# edap_ingest/ingest/parquet_ingest.py
from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import DataFrame as Spark_Dataframe

class ParquetIngest(BaseIngest):
    """
    ParquetIngest class to manage ingestion for Parquet files.
    """

    def __init__(self, lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils=None):
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
        self.lc.logger.info(f"Inside {this_module}")

    def read_data_from_source(self) -> Spark_Dataframe:
        """
        Read data from Parquet source files.
        """
        this_module = f"[{self.this_class_name}.load()] -"
        super().read_data_from_source()
        source_location = self.job_args_obj.get_mandatory("source_location")
        target_location = self.job_args_obj.get_mandatory("target_location")

        # validate extension
        self._validate_file_extension(source_location, ["parquet"])

        self.lc.logger.info(
            f"Inside {this_module} "
            f"source_location --> {source_location}, "
            f"target_location --> {target_location}"
        )

        data_df = self.spark.read.parquet(source_location)

        self.lc.logger.info(
            f"{this_module} "
            f"After reading data, data_df.columns --> {data_df.columns}"
        )
        return data_df
