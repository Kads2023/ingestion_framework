from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import DataFrame as Spark_Dataframe
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class DeltaIngest(BaseIngest):
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
        this_module = f"[{self.this_class_name}.read_data_from_source()] -"
        context_dict = self.job_args_obj.get_job_dict()
        sql_query_template = self.job_args_obj.get("sql_query", "").strip()
        where_clause_template = self.job_args_obj.get("where_clause", "").strip()
        columns = self.job_args_obj.get("columns", None)  # list of columns to select
        deduplication_conf = self.job_args_obj.get("deduplication", None)  # dict with keys and order_by
        column_mappings = self.job_args_obj.get("column_mappings", None)  # dict old_name->new_name

        if sql_query_template:
            try:
                sql_query = sql_query_template.format(**context_dict)
                self.lc.logger.info(f"{this_module} Executing sql_query --> {sql_query}")
                df = self.spark.sql(sql_query)
            except Exception as e:
                error_msg = f"{this_module} Failed to render or execute sql_query: {e}"
                self.lc.logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            source_table = self.job_args_obj.get_mandatory("source_table")
            df = self.spark.read.format("delta").table(source_table)

            if columns:
                # Project only selected columns, ignore missing silently
                available_cols = set(df.columns)
                select_cols = [c for c in columns if c in available_cols]
                if select_cols:
                    df = df.select(*select_cols)

            if where_clause_template:
                try:
                    rendered_clause = where_clause_template.format(**context_dict)
                    self.lc.logger.info(f"{this_module} Applying where_clause --> {rendered_clause}")
                    df = df.where(rendered_clause)
                except Exception as e:
                    error_msg = f"{this_module} Failed to apply where_clause: {e}"
                    self.lc.logger.error(error_msg)
                    raise ValueError(error_msg)

        # Deduplication if requested
        if deduplication_conf:
            keys = deduplication_conf.get("keys", [])
            order_by = deduplication_conf.get("order_by", [])  # list of dicts with column and direction
            if keys and order_by:
                self.lc.logger.info(f"{this_module} Applying deduplication on keys={keys} order_by={order_by}")

                # Build list of column expressions for ordering
                order_cols = []
                for order_spec in order_by:
                    col_name = order_spec.get("column")
                    direction = order_spec.get("direction", "asc").lower()
                    if direction == "desc":
                        order_cols.append(F.col(col_name).desc())
                    else:
                        order_cols.append(F.col(col_name).asc())

                window_spec = Window.partitionBy(*keys).orderBy(*order_cols)

                df = df.withColumn("_rn", F.row_number().over(window_spec)) \
                       .filter(F.col("_rn") == 1) \
                       .drop("_rn")

        # Rename columns if mapping is provided
        if column_mappings:
            for old_col, new_col in column_mappings.items():
                if old_col in df.columns and old_col != new_col:
                    df = df.withColumnRenamed(old_col, new_col)
                    self.lc.logger.info(f"{this_module} Renamed column {old_col} to {new_col}")

        self.lc.logger.info(f"{this_module} Final columns --> {df.columns}")

        # Apply custom transformations if any (list of python code strings)
        custom_transformations = self.job_args_obj.get("custom_transformations", [])
        for i, code_snippet in enumerate(custom_transformations):
            try:
                # We provide 'df' and 'F' (functions) in locals for transformations
                local_vars = {"df": df, "F": F}
                exec(code_snippet, {}, local_vars)
                df = local_vars["df"]
                self.lc.logger.info(f"{this_module} Applied custom transformation #{i+1}")
            except Exception as e:
                error_msg = f"{this_module} Failed to apply custom transformation #{i+1}: {e}"
                self.lc.logger.error(error_msg)
                raise ValueError(error_msg)

        return df

    def merge_data_to_target_table(self, source_df: Spark_Dataframe):
        this_module = f"[{self.this_class_name}.merge_data_to_target_table()] -"
        merge_conf = self.job_args_obj.get("merge", None)
        if not merge_conf:
            raise ValueError(f"{this_module} Merge configuration missing")

        target_table = merge_conf.get("target_table")
        merge_keys = merge_conf.get("merge_keys", [])
        update_columns = merge_conf.get("update_columns", [])
        delete_condition = merge_conf.get("delete_condition", None)

        if not target_table or not merge_keys:
            raise ValueError(f"{this_module} target_table and merge_keys must be specified for merge")

        self.lc.logger.info(f"{this_module} Starting merge into {target_table} on keys {merge_keys}")

        delta_table = DeltaTable.forName(self.spark, target_table)

        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

        merge_builder = (
            delta_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition=None,
                set={col: f"source.{col}" for col in update_columns}
            )
            .whenNotMatchedInsertAll()
        )

        if delete_condition:
            merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)

        merge_builder.execute()
        self.lc.logger.info(f"{this_module} Merge completed successfully")

    def write_data_to_target_table(self, data_to_write: Spark_Dataframe):
        this_module = f"[{self.this_class_name}.write_data_to_target_table()] -"
        dry_run = self.job_args_obj.get("dry_run")
        merge_conf = self.job_args_obj.get("merge", None)

        if dry_run:
            self.lc.logger.info(f"{this_module} Dry run enabled - skipping write/merge")
            return

        if merge_conf:
            # Perform merge instead of simple write
            self.merge_data_to_target_table(data_to_write)
            return

        target_location = self.job_args_obj.get_mandatory("target_location")
        write_mode = self.job_args_obj.get("write_mode", "append").lower()
        partition_by = self.job_args_obj.get("partition_by", None)  # list or None

        run_row_count = data_to_write.count()
        self.job_args_obj.set("run_row_count", run_row_count)

        if dry_run:
            self.lc.logger.info(f"{this_module} Dry run enabled - skipping write")
            return

        writer = data_to_write.write.mode(write_mode)
        if partition_by:
            if isinstance(partition_by, list) and len(partition_by) > 0:
                writer = writer.partitionBy(*partition_by)

        self.lc.logger.info(f"{this_module} Writing data to {target_location} mode={write_mode} partitionBy={partition_by}")
        writer.saveAsTable(target_location)
